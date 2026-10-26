"""s3shift.redshift_connector — the redshift_connector API (connect, cursors, fetch_dataframe, DB-API errors)
over HTTP to the s3shift Lambda URL. Standard library only (pandas only for fetch_dataframe).

    from s3shift import redshift_connector
    conn = redshift_connector.connect(host="xxx.lambda-url.us-east-2.on.aws", database="dev",
                                      user="admin", password="...")
    cur = conn.cursor()
    cur.execute("SELECT region, SUM(amount) FROM sales WHERE day >= %s GROUP BY 1 ORDER BY 2 DESC", ["2026-01-01"])
    cur.fetchall()                  # ([region, Decimal(...)], ...)
    cur.fetch_dataframe()           # pandas, if installed

Every statement is committed when execute() returns (s3shift has no sessions); commit() is a no-op.
Large results are fetched page by page inside execute(), so fetchall() is always complete.
Retry policy: a request is re-sent once only when it provably never reached the server.
"""
import base64
import datetime
import decimal
import http.client
import json
import socket
import urllib.parse
import warnings

__version__ = "0.1.0"
apilevel = "2.0"
threadsafety = 1
paramstyle = "format"


# ---------------- DB-API exceptions (redshift_connector's names) ----------------

class Warning(Exception): pass                 # noqa: A001
class Error(Exception): pass
class InterfaceError(Error): pass
class DatabaseError(Error): pass
class DataError(DatabaseError): pass
class OperationalError(DatabaseError): pass
class IntegrityError(DatabaseError): pass
class InternalError(DatabaseError): pass
class ProgrammingError(DatabaseError): pass
class NotSupportedError(DatabaseError): pass

_ERRORS = {c.__name__: c for c in (DataError, OperationalError, IntegrityError, InternalError, ProgrammingError,
                                    NotSupportedError, DatabaseError)}


class _NotDelivered(Exception):
    def __init__(self, cause, reused):
        super().__init__(cause)
        self.reused = reused


# ---------------- values ----------------

def _encode(v, i):
    if v is None or isinstance(v, (bool, int, float, str)):
        return v
    if isinstance(v, datetime.datetime):
        return {"$ts": v.isoformat(sep=" ")}
    if isinstance(v, datetime.date):
        return {"$d": v.isoformat()}
    if isinstance(v, decimal.Decimal):
        return {"$n": str(v)}
    if isinstance(v, (bytes, bytearray, memoryview)):
        return {"$b": base64.b64encode(bytes(v)).decode()}
    raise InterfaceError(f"parameter #{i}: type {type(v).__name__} cannot be sent")


def _decode(v):
    if not isinstance(v, dict) or len(v) != 1:
        return v
    (tag, text), = v.items()
    if tag == "$n":
        return decimal.Decimal(text)
    if tag == "$d":
        return datetime.date.fromisoformat(text)
    if tag == "$ts":
        return datetime.datetime.fromisoformat(text)
    if tag == "$t":
        return datetime.time.fromisoformat(text)
    if tag == "$b":
        return base64.b64decode(text)
    if tag == "$f":
        return float(text)
    return v


def _bind(sql, params):
    """%s / %(name)s -> ?, %% -> %; quoted text is left alone. Returns (sql, list of params)."""
    names, out, i, n, quote = [], [], 0, len(sql), None
    while i < n:
        ch = sql[i]
        if quote:
            out.append(ch)
            if ch == quote:
                quote = None
        elif ch in ("'", '"'):
            quote = ch
            out.append(ch)
        elif ch == "%" and i + 1 < n:
            nxt = sql[i + 1]
            if nxt == "s":
                out.append("?"); names.append(None); i += 1
            elif nxt == "%":
                out.append("%"); i += 1
            elif nxt == "(":
                end = sql.find(")s", i)
                if end == -1:
                    raise ProgrammingError(f"bad placeholder near {sql[i:i + 20]!r}")
                out.append("?"); names.append(sql[i + 2:end]); i = end + 1
            else:
                out.append(ch)
        else:
            out.append(ch)
        i += 1
    if params is None:
        if names:
            raise ProgrammingError("the statement has placeholders but no parameters were given")
        return sql, []
    if isinstance(params, dict):
        return "".join(out), [params[k] for k in names]
    params = list(params)
    if len(params) != len(names):
        raise ProgrammingError(f"{len(names)} placeholders, {len(params)} parameters")
    return "".join(out), params


# ---------------- connection ----------------

class Connection:
    def __init__(self, host, database="dev", user="", password="", timeout=900):
        url = host if host.startswith("http") else "https://" + host
        p = urllib.parse.urlparse(url.rstrip("/"))
        self._host, self._base_path = p.netloc, p.path
        self.database = database
        self._auth = base64.b64encode(f"{user}:{password}".encode()).decode()
        self.timeout = timeout
        self._conn = None
        self.autocommit = True
        self._written = False
        self.last = {}   # {"ms": server time, "scanned": bytes read from S3} of the last statement

    # ---- transport ----

    def close(self):
        try:
            if self._conn:
                self._conn.close()
        finally:
            self._conn = None

    def __enter__(self):
        return self

    def __exit__(self, *a):
        self.close()

    def _send(self, path, body):
        reused = self._conn is not None
        if self._conn is None:
            self._conn = http.client.HTTPSConnection(self._host, timeout=self.timeout)
        headers = {"content-type": "application/json", "authorization": f"Basic {self._auth}"}
        try:
            self._conn.request("POST", self._base_path + path, body, headers)
        except (socket.timeout, TimeoutError):
            raise
        except (OSError, http.client.HTTPException) as e:
            raise _NotDelivered(e, reused) from None
        try:
            resp = self._conn.getresponse()
        except (http.client.RemoteDisconnected, http.client.BadStatusLine) as e:
            raise _NotDelivered(e, reused) from None
        raw = resp.read()
        try:
            data = json.loads(raw)
        except ValueError:
            data = {"error": raw[:200].decode(errors="replace")}
        return resp.status, data

    def _post(self, path, payload):
        body = json.dumps(payload)
        try:
            status, data = self._send(path, body)
        except _NotDelivered as e:
            self.close()
            if not e.reused:
                raise InterfaceError(f"cannot reach {self._host}: {e}") from None
            status, data = self._send(path, body)
        except (socket.timeout, TimeoutError) as e:
            self.close()
            raise OperationalError(f"timeout talking to {self._host}; result unknown ({e})") from None
        if status == 401:
            raise InterfaceError("authentication failed (user / password)")
        if status >= 400:
            raise _ERRORS.get(data.get("kind"), DatabaseError)(data.get("error", f"HTTP {status}"))
        return data

    def run(self, sql, params=None):
        """One statement; returns {columns, rows (decoded, complete), rowcount, scanned, ms}."""
        qmark, args = _bind(sql, params)
        data = self._post("/api/query", {"sql": qmark, "params": [_encode(a, i + 1) for i, a in enumerate(args)],
                                         "database": self.database})
        rows = data["rows"]
        while data.get("next_token"):
            data = self._post("/api/query", {"next_token": data["next_token"]})
            rows += data["rows"]
        first = {"columns": data["columns"], "rows": [[_decode(v) for v in r] for r in rows],
                 "rowcount": data["rowcount"], "scanned": data.get("scanned"), "ms": data.get("ms")}
        self.last = {"ms": first["ms"], "scanned": first["scanned"]}
        if not first["columns"] and first["rowcount"]:
            self._written = True
        return first

    # ---- DB-API ----

    def cursor(self):
        return Cursor(self)

    def commit(self):
        self._written = False

    def rollback(self):
        if self._written:
            warnings.warn("rollback() cannot undo statements that already ran: every statement is committed "
                          "when execute() returns", RuntimeWarning, stacklevel=2)
        self._written = False


class Cursor:
    def __init__(self, connection):
        self.connection = connection
        self.description = None
        self.rowcount = -1
        self.arraysize = 1
        self._rows, self._pos = [], 0

    def execute(self, operation, args=None, stream=None, merge_socket_read=False):
        r = self.connection.run(operation, args)
        cols = r["columns"]
        self.description = [(c, None, None, None, None, None, None) for c in cols] or None
        self.rowcount = r["rowcount"]
        self._rows, self._pos = r["rows"], 0
        return self

    def executemany(self, operation, param_sets):
        total = 0
        for p in param_sets:
            self.execute(operation, p)
            total += max(self.rowcount, 0)
        self.rowcount = total
        return self

    def fetchone(self):
        if self._pos >= len(self._rows):
            return None
        row = self._rows[self._pos]
        self._pos += 1
        return row

    def fetchmany(self, num=None):
        n = num or self.arraysize
        out = self._rows[self._pos:self._pos + n]
        self._pos += len(out)
        return tuple(out)

    def fetchall(self):
        out = self._rows[self._pos:]
        self._pos = len(self._rows)
        return tuple(out)

    def fetch_dataframe(self, num=None):
        import pandas
        rows = self.fetchmany(num) if num else self.fetchall()
        return pandas.DataFrame(list(rows), columns=[d[0] for d in self.description or []])

    def __iter__(self):
        while True:
            row = self.fetchone()
            if row is None:
                return
            yield row

    def close(self):
        self._rows = []

    def __enter__(self):
        return self

    def __exit__(self, *a):
        self.close()


def connect(host, database="dev", user="", password="", port=None, timeout=900, **_ignored):
    """Same signature as redshift_connector.connect; host is the Lambda URL. Extra arguments
    (port, ssl, iam, region, ...) are accepted and ignored."""
    return Connection(host, database, user, password, timeout)
