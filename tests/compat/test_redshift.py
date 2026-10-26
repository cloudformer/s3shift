"""Redshift-only statements: PostgreSQL does not have them, so each test asserts Redshift's documented
behaviour on s3shift directly. One test = one feature; docstring "Category | Feature | note".
"""
import datetime
import os

import duckdb
import pytest

from s3shift import redshift_connector

BUCKET = os.environ["BUCKET"]


def q(conn, sql, params=None):
    cur = conn.cursor()
    cur.execute(sql, params)
    return list(cur.fetchall()) if cur.description else cur.rowcount


def put_parquet(s3, key, select):
    path = os.path.join(os.environ["S3SHIFT_CACHE"], "compat-upload.parquet")
    duckdb.execute(f"COPY ({select}) TO '{path}' (FORMAT parquet)")
    s3.upload_file(path, BUCKET, key)


# ---- DDL ----

def test_table_attributes_accepted(conn):
    """DDL | DISTSTYLE / DISTKEY / SORTKEY / COMPOUND SORTKEY / ENCODE are accepted (layout is not needed here)"""
    q(conn, "CREATE TABLE t (id INT ENCODE az64, day DATE SORTKEY, region VARCHAR(10) ENCODE zstd) "
            "DISTSTYLE KEY DISTKEY(id) COMPOUND SORTKEY(day, region)")
    q(conn, "CREATE TABLE t2 (id INT DISTKEY, x INT) DISTSTYLE EVEN")
    assert q(conn, "INSERT INTO t VALUES (1, '2026-01-01', 'us')") == 1


def test_vacuum_analyze_noop(conn):
    """DDL | VACUUM / ANALYZE / SET / BEGIN / COMMIT run as no-ops"""
    for sql in ("VACUUM", "VACUUM DELETE ONLY sales", "ANALYZE", "ANALYZE sales", "SET search_path TO public",
                "BEGIN", "COMMIT", "END"):
        assert q(conn, sql) == -1


# ---- functions ----

def test_nvl_nvl2_len(conn):
    """Functions | NVL / NVL2 / LEN"""
    assert q(conn, "SELECT NVL(NULL, 'a'), NVL('b', 'a'), NVL2(NULL, 'set', 'unset'), NVL2(1, 'set', 'unset'), LEN('abcd')") == \
        [["a", "b", "unset", "set", 4]]


def test_getdate_sysdate(conn):
    """Functions | GETDATE() / SYSDATE return a timestamp without fraction"""
    a, b = q(conn, "SELECT GETDATE(), SYSDATE")[0]
    assert isinstance(a, datetime.datetime) and a.microsecond == 0 and isinstance(b, datetime.datetime)


def test_dateadd_datediff(conn):
    """Functions | DATEADD / DATEDIFF with unquoted date parts"""
    assert q(conn, "SELECT DATEADD(day, 3, '2026-01-30'::date), DATEADD(month, 1, '2026-01-15'::date), "
                   "DATEDIFF(day, '2026-01-01'::date, '2026-03-01'::date), DATEDIFF(hour, '2026-01-01 00:00'::timestamp, "
                   "'2026-01-02 06:00'::timestamp)") == \
        [[datetime.datetime(2026, 2, 2), datetime.datetime(2026, 2, 15), 59, 30]]


def test_to_char(conn):
    """Functions | TO_CHAR with YYYY MM DD HH24 MI SS"""
    assert q(conn, "SELECT TO_CHAR('2026-03-04 05:06:07'::timestamp, 'YYYY-MM-DD HH24:MI:SS')") == [["2026-03-04 05:06:07"]]


def test_listagg(conn):
    """Functions | LISTAGG ... WITHIN GROUP (ORDER BY ...)"""
    q(conn, "CREATE TABLE p (g VARCHAR, v VARCHAR)")
    q(conn, "INSERT INTO p VALUES ('a', 'y'), ('a', 'x'), ('b', 'z')")
    assert q(conn, "SELECT g, LISTAGG(v, ',') WITHIN GROUP (ORDER BY v) FROM p GROUP BY g ORDER BY g") == [["a", "x,y"], ["b", "z"]]


def test_median_percentile(conn):
    """Functions | MEDIAN / PERCENTILE_CONT ... WITHIN GROUP"""
    assert q(conn, "SELECT MEDIAN(x), PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY x) FROM (SELECT 1 AS x UNION ALL SELECT 2 UNION ALL SELECT 4) t") == [[2.0, 2.0]]


# ---- load / unload ----

def test_copy_csv(conn, bucket):
    """Load / unload | COPY FROM 's3://...' CSV IGNOREHEADER 1 IAM_ROLE '...'"""
    q(conn, "CREATE TABLE t (id INT, name VARCHAR(10))")
    bucket.put_object(Bucket=BUCKET, Key="in/part1.csv", Body=b"id,name\n1,a\n2,b\n")
    bucket.put_object(Bucket=BUCKET, Key="in/part2.csv", Body=b"id,name\n3,c\n")
    assert q(conn, f"COPY t FROM 's3://{BUCKET}/in/' IAM_ROLE 'arn:aws:iam::123:role/r' CSV IGNOREHEADER 1 REGION 'us-east-2'") == 3
    assert q(conn, "SELECT id, name FROM t ORDER BY id") == [[1, "a"], [2, "b"], [3, "c"]]


def test_copy_pipe_delimited_default(conn, bucket):
    """Load / unload | COPY without CSV reads pipe-delimited text (Redshift's default)"""
    q(conn, "CREATE TABLE t (id INT, name VARCHAR(10))")
    bucket.put_object(Bucket=BUCKET, Key="pipe/a.txt", Body=b"1|a\n2|b\n")
    assert q(conn, f"COPY t FROM 's3://{BUCKET}/pipe/' IAM_ROLE 'arn:aws:iam::123:role/r'") == 2


def test_copy_parquet_json(conn, bucket):
    """Load / unload | COPY ... FORMAT AS PARQUET and FORMAT AS JSON 'auto'"""
    q(conn, "CREATE TABLE t (id INT, name VARCHAR(10))")
    put_parquet(bucket, "pq/a.parquet", "SELECT 1 AS id, 'a' AS name")
    assert q(conn, f"COPY t FROM 's3://{BUCKET}/pq/' FORMAT AS PARQUET") == 1
    bucket.put_object(Bucket=BUCKET, Key="js/a.json", Body=b'{"id": 2, "name": "b"}\n{"id": 3, "name": "c"}\n')
    assert q(conn, f"COPY t FROM 's3://{BUCKET}/js/' FORMAT AS JSON 'auto'") == 2
    assert q(conn, "SELECT count(*) FROM t") == [[3]]


def test_unload(conn, bucket):
    """Load / unload | UNLOAD ('...') TO 's3://...' FORMAT AS PARQUET / CSV HEADER, ALLOWOVERWRITE"""
    q(conn, "CREATE TABLE t (id INT, name VARCHAR(10))")
    q(conn, "INSERT INTO t VALUES (1, 'a'), (2, 'b')")
    assert q(conn, f"UNLOAD ('SELECT * FROM t WHERE name = ''a''') TO 's3://{BUCKET}/out/a_' FORMAT AS PARQUET") == 1
    assert q(conn, f"UNLOAD ('SELECT * FROM t') TO 's3://{BUCKET}/csv/t_' CSV HEADER") == 2
    body = bucket.get_object(Bucket=BUCKET, Key="csv/t_000.csv")["Body"].read().decode()
    assert body.splitlines() == ["id,name", "1,a", "2,b"]
    with pytest.raises(redshift_connector.ProgrammingError):
        q(conn, f"UNLOAD ('SELECT 1') TO 's3://{BUCKET}/csv/t_' CSV")
    assert q(conn, f"UNLOAD ('SELECT 1') TO 's3://{BUCKET}/csv/t_' CSV ALLOWOVERWRITE") == 1


# ---- external data ----

def test_external_table_parquet(conn, bucket):
    """External data | CREATE EXTERNAL TABLE ... STORED AS PARQUET LOCATION over many files (Spectrum)"""
    for day in ("2026-01-01", "2026-01-02"):
        put_parquet(bucket, f"lake/clicks/day={day}/part.parquet", f"SELECT 1 AS user_id, DATE '{day}' AS day")
    q(conn, f"CREATE EXTERNAL TABLE clicks (user_id INT, day DATE) STORED AS PARQUET LOCATION 's3://{BUCKET}/lake/clicks/'")
    assert q(conn, "SELECT day, count(*) FROM clicks GROUP BY day ORDER BY day") == \
        [[datetime.date(2026, 1, 1), 1], [datetime.date(2026, 1, 2), 1]]


def test_external_table_csv(conn, bucket):
    """External data | CREATE EXTERNAL TABLE ... ROW FORMAT DELIMITED ... STORED AS TEXTFILE, skip.header.line.count"""
    bucket.put_object(Bucket=BUCKET, Key="lake/users/u.csv", Body=b"id,name\n1,alice\n2,bob\n")
    q(conn, f"CREATE EXTERNAL TABLE users (id INT, name VARCHAR(20)) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' "
            f"STORED AS TEXTFILE LOCATION 's3://{BUCKET}/lake/users/' TABLE PROPERTIES ('skip.header.line.count'='1')")
    assert q(conn, "SELECT name FROM users ORDER BY id") == [["alice"], ["bob"]]


def test_external_schema_postgres(conn, real):
    """External data | CREATE EXTERNAL SCHEMA ... FROM POSTGRES: federated query joined with S3 data"""
    if real is None:
        pytest.skip("needs POSTGRES_DSN (the Docker PostgreSQL acts as the RDS instance)")
    cur = real.cursor()
    cur.execute("DROP TABLE IF EXISTS customers")
    cur.execute("CREATE TABLE customers (id INT, name VARCHAR(20))")
    cur.execute("INSERT INTO customers VALUES (1, 'alice'), (2, 'bob')")
    p = real.get_dsn_parameters()
    q(conn, f"CREATE EXTERNAL SCHEMA app FROM POSTGRES DATABASE '{p['dbname']}' URI '{p['host']}' PORT {p['port']} "
            f"USER '{p['user']}' PASSWORD 'test'")
    q(conn, "CREATE TABLE orders (customer_id INT, amount INT)")
    q(conn, "INSERT INTO orders VALUES (1, 10), (1, 5), (2, 7)")
    assert q(conn, "SELECT c.name, SUM(o.amount) FROM orders o JOIN app.customers c ON c.id = o.customer_id "
                   "GROUP BY 1 ORDER BY 1") == [["alice", 15], ["bob", 7]]


# ---- unsupported by design ----

@pytest.mark.unsupported("procedures run inside the warehouse")
def test_procedures_refused(conn):
    """Unsupported | CREATE PROCEDURE / CALL | server-side code; use application code or lambdaflow"""
    with pytest.raises(redshift_connector.NotSupportedError):
        q(conn, "CREATE PROCEDURE p() AS $$ BEGIN END $$ LANGUAGE plpgsql")


@pytest.mark.unsupported("UDFs run inside the warehouse")
def test_udf_refused(conn):
    """Unsupported | CREATE FUNCTION (SQL / Python UDF) | server-side code"""
    with pytest.raises(redshift_connector.NotSupportedError):
        q(conn, "CREATE FUNCTION f(int) RETURNS int STABLE AS $$ select $1 $$ LANGUAGE sql")


@pytest.mark.unsupported("the endpoint's user / password is the only gate")
def test_grants_refused(conn):
    """Unsupported | GRANT / CREATE USER | the endpoint credentials are the only users"""
    with pytest.raises(redshift_connector.NotSupportedError):
        q(conn, "GRANT SELECT ON sales TO bob")


@pytest.mark.unsupported("Glue Data Catalog is not read")
def test_glue_schema_refused(conn):
    """Unsupported | CREATE EXTERNAL SCHEMA FROM DATA CATALOG | declare external tables directly"""
    with pytest.raises(redshift_connector.NotSupportedError):
        q(conn, "CREATE EXTERNAL SCHEMA spectrum FROM DATA CATALOG DATABASE 'db' IAM_ROLE 'arn:aws:iam::1:role/r'")


@pytest.mark.unsupported("materialized views need a warehouse that keeps them fresh")
def test_materialized_view_refused(conn):
    """Unsupported | CREATE MATERIALIZED VIEW | use CREATE TABLE AS, refreshed by a schedule"""
    with pytest.raises(redshift_connector.NotSupportedError):
        q(conn, "CREATE MATERIALIZED VIEW mv AS SELECT 1")
