"""Redshift SQL on DuckDB, over your own S3 and RDS data. No warehouse: every request opens an in-memory
DuckDB, registers the tables the statement mentions, runs it, and is gone.

Catalog (one JSON object per database, s3://<bucket>/<database>/catalog.json, written with If-Match):
    tables   name -> {kind: managed | external | view, location, format, columns, options, sql}
    schemas  name -> {engine: postgres | mysql, host, port, database, user, password, driver, schema}

    managed    CREATE TABLE / CTAS / COPY / INSERT / UPDATE / DELETE; stored as one Parquet object
               s3://<bucket>/<database>/tables/<name>/data.parquet, replaced atomically (If-Match)
    external   CREATE EXTERNAL TABLE ... LOCATION 's3://...': files you already have (Parquet, CSV, JSON)
    view       CREATE VIEW: the SQL, expanded when a query mentions it
    schemas    CREATE EXTERNAL SCHEMA ... FROM POSTGRES | MYSQL: federated tables, read through the standard
               client (psycopg2 / pymysql, or the DRIVER you name) when a query mentions schema.table

S3 files are cached in /tmp by ETag, so a warm Lambda reads each object once.
"""
import datetime
import decimal
import importlib
import json
import os
import re
import uuid

import boto3
import duckdb
from botocore.exceptions import ClientError

CACHE = os.environ.get("S3SHIFT_CACHE", "/tmp/s3shift")


class SQLError(Exception):
    """An error reply with a DB-API class name (ProgrammingError, DataError, ...)."""
    def __init__(self, kind, message):
        super().__init__(message)
        self.kind = kind


class NotSupported(SQLError):
    def __init__(self, message):
        super().__init__("NotSupportedError", message)


class Conflict(SQLError):
    def __init__(self, message):
        super().__init__("OperationalError", message)


# ---------------- Redshift -> DuckDB ----------------

# functions Redshift has and DuckDB does not, defined once per connection
_MACROS = [
    "CREATE MACRO nvl(a, b) AS coalesce(a, b)",
    "CREATE MACRO nvl2(a, b, c) AS CASE WHEN a IS NULL THEN c ELSE b END",
    "CREATE MACRO getdate() AS date_trunc('second', current_timestamp::TIMESTAMP)",
    "CREATE MACRO len(s) AS length(s)",
    "CREATE MACRO rs_date_trunc(part, d) AS CAST(date_trunc(part, d) AS TIMESTAMP)",
    "CREATE MACRO dateadd(part, n, d) AS CAST(d AS TIMESTAMP) + to_seconds(0) + CASE lower(part)"
    " WHEN 'year' THEN to_years(n) WHEN 'month' THEN to_months(n) WHEN 'week' THEN to_weeks(n)"
    " WHEN 'day' THEN to_days(n) WHEN 'hour' THEN to_hours(n) WHEN 'minute' THEN to_minutes(n)"
    " ELSE to_seconds(n) END",
    "CREATE MACRO datediff(part, a, b) AS date_diff(part, a, b)",
    "CREATE MACRO to_char(d, f) AS strftime(d, replace(replace(replace(replace(replace(replace(replace(replace("
    "f, 'HH24', '%H'), 'HH12', '%I'), 'YYYY', '%Y'), 'MI', '%M'), 'MM', '%m'), 'DD', '%d'), 'SS', '%S'), 'Mon', '%b'))",
    "CREATE MACRO strtol(s, base) AS CASE base WHEN 16 THEN CAST(('0x' || s) AS BIGINT) ELSE CAST(s AS BIGINT) END",
]

_DATEPART_FUNCS = re.compile(r"\b(DATEADD|DATEDIFF|DATE_PART|DATEPART)\s*\(\s*([A-Za-z]+)\s*,", re.I)
_LISTAGG = re.compile(r"\bLISTAGG\s*\((.*?)\)\s*WITHIN\s+GROUP\s*\(\s*ORDER\s+BY\s+(.*?)\)", re.I | re.S)
_LISTAGG_PLAIN = re.compile(r"\bLISTAGG\s*\(", re.I)
_TABLE_ATTRS = re.compile(
    r"\s+(?:DISTSTYLE\s+\w+|(?:COMPOUND\s+|INTERLEAVED\s+)?SORTKEY\s*(?:\([^)]*\)|AUTO)|DISTKEY\s*\([^)]*\)"
    r"|ENCODE\s+\w+|BACKUP\s+(?:YES|NO))", re.I)
_COLUMN_KEYS = re.compile(r"(\w[^,()]*?)\s+(?:DISTKEY|SORTKEY)\b", re.I)
_SYSDATE = re.compile(r"\bSYSDATE\b", re.I)
_DATE_TRUNC = re.compile(r"\bDATE_TRUNC\s*\(", re.I)
_IDENT = re.compile(r'"([^"]+)"|\b([A-Za-z_][\w$]*(?:\.[A-Za-z_][\w$]*)?)\b')
_STRING = re.compile(r"'(?:[^']|'')*'")


def translate(sql):
    """Redshift spellings -> DuckDB. Small, textual, one rule per Redshift-only construct."""
    out = _DATEPART_FUNCS.sub(lambda m: f"{m.group(1)}('{m.group(2).lower()}',", sql)
    out = _LISTAGG.sub(lambda m: f"string_agg({m.group(1)} ORDER BY {m.group(2)})", out)
    out = _LISTAGG_PLAIN.sub("string_agg(", out)
    out = _SYSDATE.sub("getdate()", out)
    out = _DATE_TRUNC.sub("rs_date_trunc(", out)
    out = _TABLE_ATTRS.sub("", out)
    out = _COLUMN_KEYS.sub(lambda m: m.group(1), out)
    return out


def identifiers(sql):
    """Lower-cased identifiers and schema.table pairs outside string literals."""
    text = _STRING.sub("''", sql)
    out = set()
    for quoted, bare in _IDENT.findall(text):
        out.add((quoted or bare).lower())
    return out


def _split_columns(body):
    """'a INT, b DECIMAL(10,2)' -> ['a INT', 'b DECIMAL(10,2)'] (commas at depth 0)."""
    parts, depth, cur = [], 0, ""
    for ch in body:
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        if ch == "," and depth == 0:
            parts.append(cur.strip()); cur = ""
        else:
            cur += ch
    if cur.strip():
        parts.append(cur.strip())
    return parts


def _balanced(text):
    """'(a INT, b VARCHAR(20)) STORED AS ...' -> ('a INT, b VARCHAR(20)', 'STORED AS ...')."""
    depth = 0
    for i, ch in enumerate(text):
        depth += ch == "("
        depth -= ch == ")"
        if depth == 0:
            return text[1:i], text[i + 1:].strip()
    raise SQLError("ProgrammingError", "unbalanced parentheses in the column list")


def _quote(name):
    return ".".join(f'"{p}"' for p in name.split("."))


def _literal(s):
    return "'" + str(s).replace("'", "''") + "'"


def _s3_parts(url):
    m = re.match(r"s3://([^/]+)/?(.*)$", url)
    if not m:
        raise SQLError("ProgrammingError", f"not an s3:// location: {url}")
    return m.group(1), m.group(2)


# ---------------- statements ----------------

_NAME = r'(?:"[^"]+"|[A-Za-z_][\w$]*)(?:\.(?:"[^"]+"|[A-Za-z_][\w$]*))?'
R = lambda p: re.compile(p, re.I | re.S)  # noqa: E731
_CREATE_EXT_TABLE = R(rf"^CREATE\s+EXTERNAL\s+TABLE\s+(IF\s+NOT\s+EXISTS\s+)?({_NAME})\s*(\(.*)$")
_CREATE_EXT_SCHEMA = R(rf"^CREATE\s+EXTERNAL\s+SCHEMA\s+(IF\s+NOT\s+EXISTS\s+)?({_NAME})\s+FROM\s+(\w+)(.*)$")
_DROP_SCHEMA = R(rf"^DROP\s+SCHEMA\s+(IF\s+EXISTS\s+)?({_NAME})(\s+DROP\s+EXTERNAL\s+DATABASE)?\s*(CASCADE|RESTRICT)?$")
_CTAS = R(rf"^CREATE\s+(?:TEMP(?:ORARY)?\s+)?TABLE\s+(IF\s+NOT\s+EXISTS\s+)?({_NAME})\s*(?:\(([^()]*)\))?\s*(.*?)\bAS\s*(\(?\s*(?:SELECT|WITH)\b.*)$")
_CREATE_TABLE = R(rf"^CREATE\s+(?:TEMP(?:ORARY)?\s+)?TABLE\s+(IF\s+NOT\s+EXISTS\s+)?({_NAME})\s*\((.*)\)\s*(.*)$")
_CREATE_VIEW = R(rf"^CREATE\s+(OR\s+REPLACE\s+)?VIEW\s+({_NAME})\s+AS\s+(.*?)(\s+WITH\s+NO\s+SCHEMA\s+BINDING)?$")
_DROP = R(rf"^DROP\s+(TABLE|VIEW)\s+(IF\s+EXISTS\s+)?(.+?)\s*(CASCADE|RESTRICT)?$")
_DML = R(rf"^(?:INSERT\s+INTO|UPDATE|DELETE\s+FROM|TRUNCATE(?:\s+TABLE)?|ALTER\s+TABLE)\s+({_NAME})")
_ALTER_RENAME = R(rf"^ALTER\s+TABLE\s+({_NAME})\s+RENAME\s+TO\s+({_NAME})$")
_COPY = R(rf"^COPY\s+({_NAME})\s*(?:\(([^)]*)\))?\s+FROM\s+'([^']+)'(.*)$")
_UNLOAD = R(r"^UNLOAD\s*\(\s*'((?:[^']|'')*)'\s*\)\s*TO\s+'([^']+)'(.*)$")
_SHOW_TABLES = R(r"^SHOW\s+TABLES$")
_NOOP = R(r"^(BEGIN|START\s+TRANSACTION|COMMIT|END|ROLLBACK|ABORT|SET\s+.*|RESET\s+.*|VACUUM\b.*|ANALYZE\b.*|LOCK\b.*)$")
_UNSUPPORTED = [
    (R(r"^CREATE\s+(OR\s+REPLACE\s+)?PROCEDURE\b|^CALL\b"), "stored procedures"),
    (R(r"^CREATE\s+(OR\s+REPLACE\s+)?FUNCTION\b"), "user-defined functions"),
    (R(r"^(GRANT|REVOKE)\b|^(CREATE|ALTER|DROP)\s+(USER|GROUP|ROLE)\b"), "users and permissions (the endpoint's user / password is the only gate)"),
    (R(r"^CREATE\s+MATERIALIZED\s+VIEW\b|^REFRESH\s+MATERIALIZED\b"), "materialized views (use CREATE TABLE AS)"),
    (R(r"^CREATE\s+EXTERNAL\s+SCHEMA\b.*\bFROM\s+(DATA\s+CATALOG|HIVE\s+METASTORE|KINESIS|MSK|REDSHIFT)\b"),
     "external schemas other than POSTGRES / MYSQL (Glue Data Catalog, streams, datashares)"),
]


def _opt(text, *names):
    """A Redshift option value: FORMAT AS PARQUET, IGNOREHEADER 1, DELIMITER ',' ..."""
    for n in names:
        m = re.search(rf"\b{n}\s+(?:AS\s+)?(?:'((?:[^']|'')*)'|(\w+))", text, re.I)
        if m:
            return m.group(1) if m.group(1) is not None else m.group(2)
    return None


def _has(text, word):
    return re.search(rf"\b{word}\b", text, re.I) is not None


class Engine:
    def __init__(self, bucket, database="dev", s3=None):
        self.bucket = bucket
        self.database = database
        self.s3 = s3 or boto3.client("s3")
        self.scanned = 0
        self._catalog, self._etag = None, None

    # ---------------- catalog ----------------

    @property
    def catalog_key(self):
        return f"{self.database}/catalog.json"

    def catalog(self):
        if self._catalog is None:
            try:
                r = self.s3.get_object(Bucket=self.bucket, Key=self.catalog_key)
                self._catalog, self._etag = json.loads(r["Body"].read()), r["ETag"]
            except ClientError as e:
                if e.response["Error"]["Code"] not in ("NoSuchKey", "404"):
                    raise
                self._catalog, self._etag = {"tables": {}, "schemas": {}}, None
        return self._catalog

    def _save_catalog(self):
        kw = {"Bucket": self.bucket, "Key": self.catalog_key, "Body": json.dumps(self._catalog, indent=1).encode(),
              "ContentType": "application/json"}
        kw.update({"IfMatch": self._etag} if self._etag else {"IfNoneMatch": "*"})
        try:
            self._etag = self.s3.put_object(**kw)["ETag"]
        except ClientError as e:
            if e.response["Error"]["Code"] in ("PreconditionFailed", "ConditionalRequestConflict"):
                self._catalog = None
                raise Conflict("the catalog changed while this statement ran; run it again") from None
            raise

    def _table(self, name):
        return self.catalog()["tables"].get(name.lower().replace('"', ""))

    # ---------------- S3 files ----------------

    def _files(self, location):
        """[(local path, size)] for every data file under an s3:// location, downloaded once per ETag."""
        bucket, key = _s3_parts(location)
        out, kw = [], {"Bucket": bucket, "Prefix": key}
        while True:
            r = self.s3.list_objects_v2(**kw)
            for o in r.get("Contents", []):
                base = o["Key"].rsplit("/", 1)[-1]
                if not base or base.startswith(("_", ".")) or o["Key"].endswith("/"):
                    continue
                if key and not key.endswith("/") and o["Key"] != key and not o["Key"].startswith(key.rstrip("/") + "/"):
                    continue   # prefix match on a name, not a folder: s3://b/sales must not pick up sales_old
                etag = o["ETag"].strip('"')
                local = os.path.join(CACHE, bucket, f"{o['Key']}.{etag}")
                if not os.path.exists(local):
                    os.makedirs(os.path.dirname(local), exist_ok=True)
                    tmp = local + ".part"
                    self.s3.download_file(bucket, o["Key"], tmp)
                    os.replace(tmp, local)
                out.append((local, o["Size"]))
            if not r.get("IsTruncated"):
                return out
            kw["ContinuationToken"] = r["NextContinuationToken"]

    def _reader(self, files, fmt, options, columns):
        paths = "[" + ", ".join(_literal(p) for p, _ in files) + "]"
        if fmt == "parquet":
            return f"read_parquet({paths}, union_by_name = true)"
        if fmt == "json":
            return f"read_json_auto({paths})"
        args = [f"header = {'true' if options.get('header') else 'false'}"]
        if options.get("delimiter"):
            args.append(f"delim = {_literal(options['delimiter'])}")
        if columns:
            args.append("columns = {" + ", ".join(f"{_literal(c)}: {_literal(t)}" for c, t in columns) + "}")
        else:
            args.append("auto_detect = true")
        return f"read_csv({paths}, {', '.join(args)})"

    # ---------------- registration ----------------

    def _register(self, con, sql, seen=None):
        """Make every catalog table / view / federated table the SQL mentions visible to DuckDB."""
        seen = set() if seen is None else seen
        words = identifiers(sql)
        cat = self.catalog()
        for name, t in cat["tables"].items():
            if name in seen or name not in words:
                continue
            seen.add(name)
            self._ensure_schema(con, name)
            if t["kind"] == "view":
                self._register(con, t["sql"], seen)
                con.execute(f"CREATE VIEW {_quote(name)} AS {translate(t['sql'])}")
                continue
            files = self._files(t["location"])
            self.scanned += sum(size for _, size in files)
            if files:
                con.execute(f"CREATE VIEW {_quote(name)} AS SELECT * FROM {self._reader(files, t['format'], t.get('options', {}), t.get('columns'))}")
            else:
                cols = ", ".join(f'"{c}" {ty}' for c, ty in t.get("columns") or [["_empty", "INTEGER"]])
                con.execute(f"CREATE TABLE {_quote(name)} ({cols})")
        for word in words:
            if "." in word and word.split(".")[0] in cat["schemas"] and word not in seen:
                seen.add(word)
                self._federated(con, word, cat["schemas"][word.split(".")[0]])

    @staticmethod
    def _ensure_schema(con, name):
        if "." in name:
            con.execute(f'CREATE SCHEMA IF NOT EXISTS "{name.split(".")[0]}"')

    def _federated(self, con, word, s):
        """schema.table of an external schema: read it through the standard client into DuckDB."""
        schema, table = word.split(".", 1)
        driver = importlib.import_module(s.get("driver") or {"postgres": "psycopg2", "mysql": "pymysql"}[s["engine"]])
        kw = {"host": s["host"], "user": s.get("user", ""), "password": s.get("password", "")}
        kw["dbname" if s["engine"] == "postgres" and not s.get("driver") else "database"] = s["database"]
        if s.get("port"):
            kw["port"] = int(s["port"])
        try:
            conn = driver.connect(**kw)
        except Exception as e:  # noqa: BLE001
            raise SQLError("OperationalError", f"cannot connect to external schema {schema}: {e}") from None
        try:
            cur = conn.cursor()
            remote = f'"{s.get("schema") or "public"}"."{table}"' if s["engine"] == "postgres" else f"`{table}`"
            try:
                cur.execute(f"SELECT * FROM {remote}")
            except Exception as e:  # noqa: BLE001
                raise SQLError("ProgrammingError", f"{word}: {e}") from None
            cols = [d[0] for d in cur.description]
            rows = cur.fetchall()
        finally:
            conn.close()
        types = []
        for i in range(len(cols)):
            sample = next((r[i] for r in rows if r[i] is not None), None)
            types.append(_duck_type(sample))
        con.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
        con.execute(f'CREATE TABLE "{schema}"."{table}" (' + ", ".join(f'"{c}" {t}' for c, t in zip(cols, types)) + ")")
        if rows:
            con.executemany(f'INSERT INTO "{schema}"."{table}" VALUES (' + ", ".join("?" * len(cols)) + ")",
                            [list(r) for r in rows])

    # ---------------- connection ----------------

    @staticmethod
    def _connect():
        con = duckdb.connect()
        try:
            con.execute(f"SET home_directory = {_literal(CACHE)}")   # Lambda's HOME is read-only
        except duckdb.Error:
            pass
        con.execute("SET integer_division = true")      # Redshift / PostgreSQL: 7 / 2 = 3
        for m in _MACROS:
            con.execute(m)
        return con

    # ---------------- run ----------------

    def run(self, sql, params=()):
        """-> {columns, rows, rowcount}; rows are lists of Python values."""
        self.scanned = 0
        sql = "\n".join(l for l in sql.strip().splitlines() if not l.strip().startswith("--")).strip().rstrip(";").strip()
        if not sql:
            return _done()
        for pattern, what in _UNSUPPORTED:
            if pattern.search(sql):
                raise NotSupported(f"{what} are not supported")
        if _NOOP.match(sql):
            return _done()
        if _SHOW_TABLES.match(sql):
            rows = [[n, t["kind"], t.get("location", "")] for n, t in sorted(self.catalog()["tables"].items())]
            return {"columns": ["table_name", "kind", "location"], "rows": rows, "rowcount": len(rows)}
        for pattern, handler in ((_CREATE_EXT_TABLE, self._create_external_table), (_CREATE_EXT_SCHEMA, self._create_external_schema),
                                 (_DROP_SCHEMA, self._drop_schema), (_CREATE_VIEW, self._create_view), (_CTAS, self._ctas),
                                 (_CREATE_TABLE, self._create_table), (_DROP, self._drop), (_ALTER_RENAME, self._rename),
                                 (_COPY, self._copy), (_UNLOAD, self._unload)):
            m = pattern.match(sql)
            if m:
                return handler(m, params)
        m = _DML.match(sql)
        if m:
            return self._dml(m, sql, params)
        return self._query(sql, params)

    def _query(self, sql, params):
        con = self._connect()
        try:
            self._register(con, sql)
            cur = _exec(con, translate(sql), params)
            if cur.description is None:
                return _done()
            cols = [d[0] for d in cur.description]
            rows = [list(r) for r in cur.fetchall()]
            return {"columns": cols, "rows": rows, "rowcount": len(rows)}
        finally:
            con.close()

    # ---------------- DDL ----------------

    def _managed_location(self, name):
        return f"s3://{self.bucket}/{self.database}/tables/{name}/data.parquet"

    def _create_external_table(self, m, params):
        if_not, name = m.group(1), m.group(2).lower().replace('"', "")
        body, rest = _balanced(m.group(3))
        if self._table(name):
            if if_not:
                return _done()
            raise SQLError("ProgrammingError", f'relation "{name}" already exists')
        location = _opt(rest, "LOCATION")
        if not location:
            raise SQLError("ProgrammingError", "CREATE EXTERNAL TABLE needs LOCATION 's3://...'")
        stored = (_opt(rest, "STORED") or "parquet").lower()
        fmt = {"parquet": "parquet", "textfile": "csv", "csv": "csv", "json": "json"}.get(stored, stored)
        if fmt not in ("parquet", "csv", "json"):
            raise NotSupported(f"STORED AS {stored} is not supported (PARQUET, TEXTFILE / CSV, JSON)")
        columns = []
        for col in _split_columns(body):
            parts = col.split(None, 1)
            columns.append([parts[0].strip('"').lower(), parts[1] if len(parts) > 1 else "VARCHAR"])
        delimiter = None
        dm = re.search(r"FIELDS\s+TERMINATED\s+BY\s+'((?:[^']|'')*)'", rest, re.I)
        if dm:
            delimiter = dm.group(1).replace("\\t", "\t")
        elif fmt == "csv":
            delimiter = ","
        header = bool(re.search(r"'skip\.header\.line\.count'\s*=\s*'1'", rest, re.I))
        self.catalog()["tables"][name] = {"kind": "external", "location": location, "format": fmt, "columns": columns,
                                          "options": {"delimiter": delimiter, "header": header}}
        self._save_catalog()
        return _done()

    def _create_external_schema(self, m, params):
        if_not, name, engine, rest = m.group(1), m.group(2).lower().replace('"', ""), m.group(3).lower(), m.group(4)
        if engine not in ("postgres", "mysql"):
            raise NotSupported(f"external schemas FROM {engine.upper()} are not supported (POSTGRES, MYSQL)")
        if name in self.catalog()["schemas"]:
            if if_not:
                return _done()
            raise SQLError("ProgrammingError", f'schema "{name}" already exists')
        if _opt(rest, "SECRET_ARN") and not _opt(rest, "USER"):
            raise NotSupported("SECRET_ARN is not supported yet; give USER '...' PASSWORD '...'")
        uri = _opt(rest, "URI")
        database = _opt(rest, "DATABASE")
        if not uri or not database:
            raise SQLError("ProgrammingError", "CREATE EXTERNAL SCHEMA needs DATABASE '...' and URI '...'")
        self.catalog()["schemas"][name] = {
            "engine": engine, "host": uri, "port": _opt(rest, "PORT"), "database": database,
            "schema": _opt(rest, "SCHEMA"), "user": _opt(rest, "USER") or "", "password": _opt(rest, "PASSWORD") or "",
            "driver": _opt(rest, "DRIVER")}
        self._save_catalog()
        return _done()

    def _drop_schema(self, m, params):
        name = m.group(2).lower().replace('"', "")
        if name not in self.catalog()["schemas"]:
            if m.group(1):
                return _done()
            raise SQLError("ProgrammingError", f'schema "{name}" does not exist')
        del self.catalog()["schemas"][name]
        self._save_catalog()
        return _done()

    def _create_view(self, m, params):
        name = m.group(2).lower().replace('"', "")
        existing = self._table(name)
        if existing and not (m.group(1) and existing["kind"] == "view"):
            raise SQLError("ProgrammingError", f'relation "{name}" already exists')
        self._query(f"SELECT * FROM ({m.group(3)}) LIMIT 0", ())    # validate now, like Redshift
        self.catalog()["tables"][name] = {"kind": "view", "sql": m.group(3)}
        self._save_catalog()
        return _done()

    def _create_table(self, m, params):
        if_not, name, body = m.group(1), m.group(2).lower().replace('"', ""), m.group(3)
        if self._table(name):
            if if_not:
                return _done()
            raise SQLError("ProgrammingError", f'relation "{name}" already exists')
        con = self._connect()
        try:
            self._ensure_schema(con, name)
            _exec(con, translate(f"CREATE TABLE {_quote(name)} ({body})"), ())
            self._write_managed(con, name, None)
        finally:
            con.close()
        return _done()

    def _ctas(self, m, params):
        if_not, name, query = m.group(1), m.group(2).lower().replace('"', ""), m.group(5)
        if self._table(name):
            if if_not:
                return _done()
            raise SQLError("ProgrammingError", f'relation "{name}" already exists')
        con = self._connect()
        try:
            self._register(con, query)
            self._ensure_schema(con, name)
            _exec(con, f"CREATE TABLE {_quote(name)} AS {translate(query)}", params)
            n = con.execute(f"SELECT count(*) FROM {_quote(name)}").fetchone()[0]
            self._write_managed(con, name, None)
        finally:
            con.close()
        return _done(n)

    def _drop(self, m, params):
        kind, if_exists = m.group(1).lower(), m.group(2)
        for raw in _split_columns(m.group(3)):
            name = raw.lower().replace('"', "")
            t = self._table(name)
            if t is None or (kind == "view") != (t["kind"] == "view"):
                if if_exists:
                    continue
                raise SQLError("ProgrammingError", f'{kind} "{name}" does not exist')
            if t["kind"] == "managed":
                bucket, key = _s3_parts(t["location"])
                self.s3.delete_object(Bucket=bucket, Key=key)
            del self.catalog()["tables"][name]
        self._save_catalog()
        return _done()

    def _rename(self, m, params):
        old, new = m.group(1).lower().replace('"', ""), m.group(2).lower().replace('"', "")
        t = self._table(old)
        if t is None:
            raise SQLError("ProgrammingError", f'relation "{old}" does not exist')
        if self._table(new):
            raise SQLError("ProgrammingError", f'relation "{new}" already exists')
        tables = self.catalog()["tables"]
        tables[new] = tables.pop(old)        # the data stays where it is; only the name changes
        self._save_catalog()
        return _done()

    # ---------------- writes to managed tables ----------------

    def _managed(self, name):
        t = self._table(name)
        if t is None:
            raise SQLError("ProgrammingError", f'relation "{name}" does not exist')
        if t["kind"] != "managed":
            raise SQLError("ProgrammingError", f'"{name}" is {"a view" if t["kind"] == "view" else "an external table"}; '
                           "only tables created with CREATE TABLE can be written")
        return t

    def _load_managed(self, con, name, t):
        """A real DuckDB table holding the managed table's rows; returns the ETag it was read at."""
        bucket, key = _s3_parts(t["location"])
        try:
            head = self.s3.head_object(Bucket=bucket, Key=key)
        except ClientError:
            head = None
        self._ensure_schema(con, name)
        if head is None:
            cols = ", ".join(f'"{c}" {ty}' for c, ty in t["columns"])
            con.execute(f"CREATE TABLE {_quote(name)} ({cols})")
            return None
        files = self._files(t["location"])
        self.scanned += sum(size for _, size in files)
        con.execute(f"CREATE TABLE {_quote(name)} AS SELECT * FROM read_parquet({_literal(files[0][0])})")
        return head["ETag"]

    def _write_managed(self, con, name, etag):
        """Replace the table's Parquet object (If-Match the version it was read at) and its catalog entry."""
        location = self._managed_location(name)
        bucket, key = _s3_parts(location)
        os.makedirs(CACHE, exist_ok=True)
        tmp = os.path.join(CACHE, f"write-{uuid.uuid4().hex}.parquet")
        try:
            con.execute(f"COPY {_quote(name)} TO {_literal(tmp)} (FORMAT parquet)")
            with open(tmp, "rb") as f:
                kw = {"Bucket": bucket, "Key": key, "Body": f.read()}
            kw.update({"IfMatch": etag} if etag else {})
            try:
                self.s3.put_object(**kw)
            except ClientError as e:
                if e.response["Error"]["Code"] in ("PreconditionFailed", "ConditionalRequestConflict"):
                    raise Conflict(f'"{name}" was changed by another statement; run it again') from None
                raise
        finally:
            if os.path.exists(tmp):
                os.remove(tmp)
        columns = [[r[0], r[1]] for r in con.execute(f"DESCRIBE {_quote(name)}").fetchall()]
        entry = self.catalog()["tables"].get(name)
        if entry is None or entry.get("columns") != columns or entry.get("location") != location:
            self.catalog()["tables"][name] = {"kind": "managed", "location": location, "format": "parquet", "columns": columns}
            self._save_catalog()

    def _dml(self, m, sql, params):
        name = m.group(1).lower().replace('"', "")
        t = self._managed(name)
        con = self._connect()
        try:
            etag = self._load_managed(con, name, t)
            self._register(con, sql, seen={name})
            cur = _exec(con, translate(sql), params)
            row = cur.fetchone() if cur.description else None
            n = row[0] if row else -1      # ALTER / TRUNCATE: no count, like PostgreSQL
            self._write_managed(con, name, etag)
        finally:
            con.close()
        return _done(n if isinstance(n, int) else -1)

    def _copy(self, m, params):
        name, cols, source, rest = m.group(1).lower().replace('"', ""), m.group(2), m.group(3), m.group(4)
        t = self._managed(name)
        fmt = (_opt(rest, "FORMAT") or ("csv" if _has(rest, "CSV") else "parquet" if _has(rest, "PARQUET") else
                                        "json" if _has(rest, "JSON") else "csv")).lower()
        if fmt not in ("parquet", "csv", "json"):
            raise NotSupported(f"COPY ... FORMAT {fmt.upper()} is not supported (PARQUET, CSV, JSON)")
        files = self._files(source)
        if not files:
            raise SQLError("DataError", f"COPY: no files under {source}")
        self.scanned += sum(size for _, size in files)
        ignore = int(_opt(rest, "IGNOREHEADER") or 0)
        delimiter = _opt(rest, "DELIMITER") or ("|" if fmt == "csv" and not _has(rest, "CSV") else ",")
        con = self._connect()
        try:
            etag = self._load_managed(con, name, t)
            target_cols = [c.strip().strip('"') for c in cols.split(",")] if cols else [r[0] for r in con.execute(f"DESCRIBE {_quote(name)}").fetchall()]
            if fmt == "csv":
                types = dict(con.execute(f"SELECT column_name, column_type FROM (DESCRIBE {_quote(name)})").fetchall())
                columns = [[c, types.get(c, "VARCHAR")] for c in target_cols]
                reader = self._reader(files, "csv", {"header": ignore > 0, "delimiter": delimiter}, columns)
            else:
                reader = self._reader(files, fmt, {}, None)
            collist = ", ".join(f'"{c}"' for c in target_cols)
            select = collist if fmt != "csv" else "*"
            before = con.execute(f"SELECT count(*) FROM {_quote(name)}").fetchone()[0]
            _exec(con, f"INSERT INTO {_quote(name)} ({collist}) SELECT {select} FROM {reader}", ())
            n = con.execute(f"SELECT count(*) FROM {_quote(name)}").fetchone()[0] - before
            self._write_managed(con, name, etag)
        finally:
            con.close()
        return _done(n)

    def _unload(self, m, params):
        query, target, rest = m.group(1).replace("''", "'"), m.group(2), m.group(3)
        fmt = (_opt(rest, "FORMAT") or ("csv" if _has(rest, "CSV") else "parquet" if _has(rest, "PARQUET") else "csv")).lower()
        if fmt not in ("parquet", "csv", "json"):
            raise NotSupported(f"UNLOAD ... FORMAT {fmt.upper()} is not supported (PARQUET, CSV, JSON)")
        bucket, prefix = _s3_parts(target)
        key = f"{prefix}000.{fmt}" if prefix.endswith("/") or not prefix else f"{prefix}000.{fmt}"
        if not _has(rest, "ALLOWOVERWRITE"):
            r = self.s3.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1)
            if r.get("KeyCount"):
                raise SQLError("ProgrammingError", f"UNLOAD: {target} is not empty; add ALLOWOVERWRITE")
        con = self._connect()
        os.makedirs(CACHE, exist_ok=True)
        tmp = os.path.join(CACHE, f"unload-{uuid.uuid4().hex}.{fmt}")
        try:
            self._register(con, query)
            n = _exec(con, f"SELECT count(*) FROM ({translate(query)})", ()).fetchone()[0]
            opts = {"parquet": "FORMAT parquet", "json": "FORMAT json",
                    "csv": f"FORMAT csv, HEADER {'true' if _has(rest, 'HEADER') else 'false'}, "
                           f"DELIMITER {_literal(_opt(rest, 'DELIMITER') or ('|' if not _has(rest, 'CSV') else ','))}"}[fmt]
            _exec(con, f"COPY ({translate(query)}) TO {_literal(tmp)} ({opts})", ())
            self.s3.upload_file(tmp, bucket, key)
        finally:
            con.close()
            if os.path.exists(tmp):
                os.remove(tmp)
        return _done(n)


def _done(n=-1):
    """A statement without a result set. DDL and no-ops report -1, as PostgreSQL / redshift_connector do;
    INSERT / UPDATE / DELETE / CTAS / COPY / UNLOAD report the rows they touched."""
    return {"columns": [], "rows": [], "rowcount": n}


def _exec(con, sql, params):
    try:
        return con.execute(sql, list(params) if params else None)
    except duckdb.Error as e:
        raise _sql_error(e) from None


def _sql_error(e):
    name = type(e).__name__
    kind = {"ParserException": "ProgrammingError", "CatalogException": "ProgrammingError", "BinderException": "ProgrammingError",
            "ConversionException": "DataError", "OutOfRangeException": "DataError", "InvalidInputException": "DataError",
            "ConstraintException": "IntegrityError", "NotImplementedException": "NotSupportedError",
            "OutOfMemoryException": "OperationalError"}.get(name, "DatabaseError")
    msg = str(e).split("\n\nLINE")[0].replace("\n", " ")
    return SQLError(kind, msg)


def _duck_type(v):
    if isinstance(v, bool):
        return "BOOLEAN"
    if isinstance(v, int):
        return "BIGINT"
    if isinstance(v, float):
        return "DOUBLE"
    if isinstance(v, decimal.Decimal):
        return "DECIMAL(38, 10)"
    if isinstance(v, datetime.datetime):
        return "TIMESTAMP"
    if isinstance(v, datetime.date):
        return "DATE"
    if isinstance(v, datetime.time):
        return "TIME"
    if isinstance(v, (bytes, bytearray, memoryview)):
        return "BLOB"
    return "VARCHAR"
