"""The server and engine against a mocked S3: auth, managed tables, external tables, COPY / UNLOAD, views,
federated schemas, paging, conflicts."""
import base64
import datetime
import decimal
import io
import json
import os
import sqlite3
import sys
import types
import warnings

import duckdb
import pytest

import lambda_function as lf
from s3shift import redshift_connector

AUTH = "Basic " + base64.b64encode(b"admin:pw").decode()
BUCKET = os.environ["BUCKET"]


def call(method, path, body=None, auth=AUTH):
    event = {"requestContext": {"http": {"method": method}}, "rawPath": path,
             "headers": {"authorization": auth} if auth else {}, "body": json.dumps(body) if body else None}
    resp = lf.lambda_handler(event, None)
    ct = resp["headers"]["content-type"]
    return resp["statusCode"], json.loads(resp["body"]) if ct.startswith("application/json") else resp["body"]


def q(conn, sql, params=None):
    cur = conn.cursor()
    cur.execute(sql, params)
    return list(cur.fetchall()) if cur.description else cur.rowcount


def put_parquet(s3, key, select):
    path = os.path.join(os.environ["S3SHIFT_CACHE"], "upload.parquet")
    duckdb.execute(f"COPY ({select}) TO '{path}' (FORMAT parquet)")
    s3.upload_file(path, BUCKET, key)


def test_console_is_public_and_api_needs_auth(bucket):
    status, body = call("GET", "/", auth=None)
    assert status == 200 and "<html" in body
    assert call("POST", "/api/query", {"sql": "SELECT 1"}, auth=None)[0] == 401
    status, data = call("POST", "/api/query", {"sql": "SELECT 1 AS one"})
    assert status == 200 and data["rows"] == [[1]] and data["columns"] == ["one"]


def test_managed_table_lifecycle(conn):
    q(conn, "CREATE TABLE sales (id INT, region VARCHAR(20), amount DECIMAL(10,2), day DATE) DISTKEY(id) SORTKEY(day)")
    assert q(conn, "INSERT INTO sales VALUES (1, 'us', 10.50, '2026-01-01'), (2, 'eu', 20.00, '2026-01-02')") == 2
    assert q(conn, "INSERT INTO sales VALUES (%s, %s, %s, %s)", [3, "us", decimal.Decimal("5.25"), datetime.date(2026, 1, 3)]) == 1
    rows = q(conn, "SELECT region, SUM(amount) FROM sales GROUP BY region ORDER BY region")
    assert rows == [["eu", decimal.Decimal("20.00")], ["us", decimal.Decimal("15.75")]]
    assert q(conn, "UPDATE sales SET amount = amount * 2 WHERE region = 'eu'") == 1
    assert q(conn, "DELETE FROM sales WHERE id = 3") == 1
    assert q(conn, "SELECT id, amount, day FROM sales ORDER BY id") == [
        [1, decimal.Decimal("10.50"), datetime.date(2026, 1, 1)], [2, decimal.Decimal("40.00"), datetime.date(2026, 1, 2)]]
    q(conn, "CREATE TABLE big_eu AS SELECT * FROM sales WHERE region = 'eu'")
    assert q(conn, "SELECT count(*) FROM big_eu") == [[1]]
    q(conn, "ALTER TABLE big_eu RENAME TO eu_only")
    assert q(conn, "SELECT id FROM eu_only") == [[2]]
    names = [r[0] for r in q(conn, "SHOW TABLES")]
    assert names == ["eu_only", "sales"]
    q(conn, "DROP TABLE sales, eu_only")
    with pytest.raises(redshift_connector.ProgrammingError):
        q(conn, "SELECT * FROM sales")


def test_external_parquet_and_csv(conn, bucket):
    put_parquet(bucket, "lake/events/2026-01-01.parquet", "SELECT 1 AS id, 'click' AS kind UNION ALL SELECT 2, 'view'")
    put_parquet(bucket, "lake/events/2026-01-02.parquet", "SELECT 3 AS id, 'click' AS kind")
    bucket.put_object(Bucket=BUCKET, Key="lake/events/_SUCCESS", Body=b"")
    bucket.put_object(Bucket=BUCKET, Key="lake/events_old/x.parquet", Body=b"not parquet")   # must not be picked up
    q(conn, f"CREATE EXTERNAL TABLE events (id INT, kind VARCHAR) STORED AS PARQUET LOCATION 's3://{BUCKET}/lake/events/'")
    assert q(conn, "SELECT kind, count(*) FROM events GROUP BY kind ORDER BY kind") == [["click", 2], ["view", 1]]
    assert conn.last["scanned"] > 0
    bucket.put_object(Bucket=BUCKET, Key="lake/users/users.csv", Body=b"id,name\n1,alice\n2,bob\n")
    q(conn, f"CREATE EXTERNAL TABLE users (id INT, name VARCHAR) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' "
            f"STORED AS TEXTFILE LOCATION 's3://{BUCKET}/lake/users/' TABLE PROPERTIES ('skip.header.line.count'='1')")
    assert q(conn, "SELECT u.name, count(*) FROM events e JOIN users u ON u.id = e.id GROUP BY 1 ORDER BY 1") == [["alice", 1], ["bob", 1]]
    with pytest.raises(redshift_connector.ProgrammingError):
        q(conn, "INSERT INTO events VALUES (9, 'x')")   # external tables are read-only


def test_copy_and_unload(conn, bucket):
    q(conn, "CREATE TABLE t (id INT, name VARCHAR)")
    bucket.put_object(Bucket=BUCKET, Key="in/a.csv", Body=b"id,name\n1,a\n2,b\n")
    assert q(conn, f"COPY t FROM 's3://{BUCKET}/in/' IAM_ROLE 'arn:aws:iam::1:role/x' CSV IGNOREHEADER 1") == 2
    put_parquet(bucket, "in2/p.parquet", "SELECT 3 AS id, 'c' AS name")
    assert q(conn, f"COPY t FROM 's3://{BUCKET}/in2/' FORMAT AS PARQUET") == 1
    assert q(conn, "SELECT id, name FROM t ORDER BY id") == [[1, "a"], [2, "b"], [3, "c"]]
    assert q(conn, f"UNLOAD ('SELECT * FROM t WHERE name <> ''b''') TO 's3://{BUCKET}/out/t_' FORMAT AS PARQUET") == 2
    keys = [o["Key"] for o in bucket.list_objects_v2(Bucket=BUCKET, Prefix="out/")["Contents"]]
    assert keys == ["out/t_000.parquet"]
    with pytest.raises(redshift_connector.ProgrammingError):
        q(conn, f"UNLOAD ('SELECT 1') TO 's3://{BUCKET}/out/t_' FORMAT AS PARQUET")   # not empty, no ALLOWOVERWRITE
    q(conn, f"UNLOAD ('SELECT 1 AS x') TO 's3://{BUCKET}/out/t_' FORMAT AS PARQUET ALLOWOVERWRITE")


def test_views_and_redshift_functions(conn):
    q(conn, "CREATE TABLE o (id INT, region VARCHAR, day DATE)")
    q(conn, "INSERT INTO o VALUES (1, 'us', '2026-01-01'), (2, 'us', '2026-01-05'), (3, 'eu', '2026-02-01')")
    q(conn, "CREATE VIEW us_orders AS SELECT * FROM o WHERE region = 'us'")
    assert q(conn, "SELECT count(*) FROM us_orders") == [[2]]
    row = q(conn, "SELECT NVL(NULL, 'x'), LEN('abc'), DATEDIFF(day, '2026-01-01'::date, '2026-01-10'::date), "
                  "TO_CHAR('2026-03-04'::date, 'YYYY-MM'), 7 / 2, "
                  "LISTAGG(region, ',') WITHIN GROUP (ORDER BY region) FROM (SELECT DISTINCT region FROM o) x")[0]
    assert row == ["x", 3, 9, "2026-03", 3, "eu,us"]
    assert q(conn, "SELECT DATEADD(day, 3, '2026-01-01'::date)")[0][0] == datetime.datetime(2026, 1, 4)
    assert isinstance(q(conn, "SELECT GETDATE(), SYSDATE")[0][0], datetime.datetime)


def test_federated_schema_through_the_standard_client(conn, monkeypatch):
    """CREATE EXTERNAL SCHEMA ... FROM MYSQL reads through the DRIVER module (here a stand-in on sqlite3)."""
    db = sqlite3.connect(":memory:", check_same_thread=False)
    db.execute("CREATE TABLE customers (id INTEGER, name TEXT)")
    db.execute("INSERT INTO customers VALUES (1, 'alice'), (2, 'bob')")
    seen = {}

    class Conn:
        def cursor(self):
            return db.cursor()

        def close(self):
            pass

    fake = types.ModuleType("fake_mysql_driver")
    fake.connect = lambda **kw: seen.update(kw) or Conn()
    monkeypatch.setitem(sys.modules, "fake_mysql_driver", fake)
    real_cursor = db.cursor
    db_cursor = lambda: _Backtick(real_cursor())  # noqa: E731
    Conn.cursor = lambda self: db_cursor()
    q(conn, "CREATE EXTERNAL SCHEMA shop FROM MYSQL DATABASE 'shop' URI 'db.example' PORT 3306 "
            "USER 'reader' PASSWORD 'pw' DRIVER 'fake_mysql_driver'")
    q(conn, "CREATE TABLE orders (customer_id INT, amount INT)")
    q(conn, "INSERT INTO orders VALUES (1, 10), (1, 5), (2, 7)")
    rows = q(conn, "SELECT c.name, SUM(o.amount) FROM orders o JOIN shop.customers c ON c.id = o.customer_id GROUP BY 1 ORDER BY 1")
    assert rows == [["alice", 15], ["bob", 7]]
    assert seen == {"host": "db.example", "user": "reader", "password": "pw", "database": "shop", "port": 3306}
    status, data = call("GET", "/api/tables")
    assert "password" not in data["schemas"]["shop"]


class _Backtick:
    """sqlite3 cursor that accepts MySQL backtick quoting, enough for the stand-in driver."""
    def __init__(self, cur):
        self.cur = cur

    def execute(self, sql):
        return self.cur.execute(sql.replace("`", '"'))

    @property
    def description(self):
        return self.cur.description

    def fetchall(self):
        return self.cur.fetchall()


def test_large_results_are_paged(conn, monkeypatch):
    monkeypatch.setattr(lf, "PAGE_BYTES", 2000)
    rows = q(conn, "SELECT i, 'row-' || i AS label FROM range(500) t(i) ORDER BY i")
    assert len(rows) == 500 and rows[-1] == [499, "row-499"]


def test_errors_and_unsupported(conn):
    with pytest.raises(redshift_connector.ProgrammingError):
        q(conn, "SELEC 1")
    with pytest.raises(redshift_connector.ProgrammingError):
        q(conn, "SELECT nope FROM nowhere")
    for sql in ("CREATE PROCEDURE p() AS $$ BEGIN END $$ LANGUAGE plpgsql", "GRANT SELECT ON t TO bob",
                "CREATE FUNCTION f(int) RETURNS int AS 'select $1' LANGUAGE sql",
                "CREATE EXTERNAL SCHEMA s FROM DATA CATALOG DATABASE 'x' IAM_ROLE 'y'"):
        with pytest.raises(redshift_connector.NotSupportedError):
            q(conn, sql)
    assert q(conn, "BEGIN") == -1 and q(conn, "VACUUM") == -1 and q(conn, "COMMIT") == -1


def test_concurrent_writes_conflict_instead_of_losing_data(conn, bucket):
    q(conn, "CREATE TABLE c (n INT)")
    q(conn, "INSERT INTO c VALUES (1)")
    import engine as eng
    original = eng.Engine._write_managed

    def sneaky(self, con, name, etag):
        bucket.put_object(Bucket=BUCKET, Key=f"dev/tables/{name}/data.parquet", Body=b"someone else")
        return original(self, con, name, etag)

    eng.Engine._write_managed = sneaky
    try:
        with pytest.raises(redshift_connector.OperationalError):
            q(conn, "INSERT INTO c VALUES (2)")
    finally:
        eng.Engine._write_managed = original


def test_rollback_warns_after_a_write(conn):
    q(conn, "CREATE TABLE w (n INT)")
    q(conn, "INSERT INTO w VALUES (1)")
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        conn.rollback()
    assert any("cannot undo" in str(x.message) for x in w)
