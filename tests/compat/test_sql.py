"""s3shift vs a real PostgreSQL 16: the SQL Redshift inherited, statement by statement.

One test = one feature; docstring "Category | Feature | note". The list only grows. Run with:

    docker compose -f tests/compat/docker-compose.yml up -d postgres
    POSTGRES_DSN=postgresql://postgres:test@127.0.0.1:5433/compat python3 -m pytest tests -q
    COMPAT_DRY=1 python3 -m pytest tests/compat -q -s      # s3shift side only, no Docker
"""
import datetime
import decimal

import duckdb
import pytest

SALES = [
    "DROP TABLE IF EXISTS sales",
    "CREATE TABLE sales (id INT, region VARCHAR(20), product VARCHAR(20), amount DECIMAL(10,2), qty INT, day DATE)",
    "INSERT INTO sales VALUES (1, 'us', 'apple', 10.50, 3, '2026-01-01'), (2, 'us', 'pear', 4.00, 1, '2026-01-02'), "
    "(3, 'eu', 'apple', 7.25, 2, '2026-01-02'), (4, 'eu', NULL, 1.00, 5, '2026-02-01'), (5, 'asia', 'kiwi', 12.00, 4, '2026-02-03')",
]


# ---- DDL ----

def test_create_insert_select(pair):
    """DDL | CREATE TABLE with INT / BIGINT / DECIMAL / DOUBLE / VARCHAR / DATE / TIMESTAMP / BOOLEAN; DROP TABLE"""
    pair(["DROP TABLE IF EXISTS t",
          "CREATE TABLE t (i INT, b BIGINT, d DECIMAL(10,2), f DOUBLE PRECISION, s VARCHAR(10), dt DATE, ts TIMESTAMP, ok BOOLEAN)",
          "INSERT INTO t VALUES (1, 9007199254740993, 1.25, 2.5, 'x', '2026-01-02', '2026-01-02 03:04:05', true)",
          "SELECT i, b, d, f, s, dt, ts, ok FROM t",
          "DROP TABLE t"])


def test_ctas(pair):
    """DDL | CREATE TABLE ... AS SELECT"""
    pair(SALES + ["DROP TABLE IF EXISTS us_sales", "CREATE TABLE us_sales AS SELECT id, amount FROM sales WHERE region = 'us'",
                  "SELECT id, amount FROM us_sales ORDER BY id", "DROP TABLE us_sales"])


def test_alter_add_drop_column(pair):
    """DDL | ALTER TABLE ADD COLUMN / DROP COLUMN"""
    pair(["DROP TABLE IF EXISTS t", "CREATE TABLE t (id INT)", "INSERT INTO t VALUES (1)",
          "ALTER TABLE t ADD COLUMN note VARCHAR(10)", "SELECT id, note FROM t",
          "ALTER TABLE t DROP COLUMN note", "SELECT * FROM t"])


def test_views(pair):
    """DDL | CREATE VIEW / CREATE OR REPLACE VIEW / DROP VIEW"""
    pair(SALES + ["DROP VIEW IF EXISTS v", "CREATE VIEW v AS SELECT region, amount FROM sales WHERE amount > 5",
                  "SELECT region, amount FROM v ORDER BY amount", "CREATE OR REPLACE VIEW v AS SELECT region, amount FROM sales WHERE id < 3",
                  "SELECT region, amount FROM v ORDER BY amount", "DROP VIEW v"])


# ---- DML ----

def test_insert_update_delete_rowcount(pair):
    """DML | INSERT (multi-row, with parameters) / UPDATE / DELETE and their rowcount"""
    pair(SALES + [("INSERT INTO sales (id, region, amount) VALUES (%s, %s, %s)", [6, "us", decimal.Decimal("3.30")]),
                  "UPDATE sales SET qty = qty + 1 WHERE region = 'us'",
                  "DELETE FROM sales WHERE amount < 2",
                  "SELECT id, qty FROM sales ORDER BY id"])


def test_insert_select(pair):
    """DML | INSERT INTO ... SELECT"""
    pair(SALES + ["DROP TABLE IF EXISTS archive", "CREATE TABLE archive (id INT, amount DECIMAL(10,2))",
                  "INSERT INTO archive SELECT id, amount FROM sales WHERE day < '2026-02-01'",
                  "SELECT id, amount FROM archive ORDER BY id"])


# ---- queries ----

def test_where_and_order(pair):
    """Queries | WHERE with IN / BETWEEN / LIKE / ILIKE / IS NULL; ORDER BY with NULLS FIRST / LAST"""
    pair(SALES + ["SELECT id FROM sales WHERE region IN ('us', 'asia') AND amount BETWEEN 4 AND 11 ORDER BY id",
                  "SELECT id FROM sales WHERE product LIKE 'ap%' ORDER BY id",
                  "SELECT id FROM sales WHERE product ILIKE 'APP%' ORDER BY id",
                  "SELECT id FROM sales WHERE product IS NULL",
                  "SELECT id, product FROM sales ORDER BY product NULLS FIRST, id",
                  "SELECT id, product FROM sales ORDER BY product DESC NULLS LAST, id"])


def test_group_having_distinct_limit(pair):
    """Queries | GROUP BY / HAVING, COUNT(DISTINCT), LIMIT / OFFSET"""
    pair(SALES + ["SELECT region, count(*), SUM(qty) FROM sales GROUP BY region HAVING count(*) > 1 ORDER BY region",
                  "SELECT count(DISTINCT region) FROM sales",
                  "SELECT id FROM sales ORDER BY id LIMIT 2 OFFSET 1"])


def test_joins(pair):
    """Queries | INNER / LEFT / FULL OUTER JOIN"""
    pair(SALES + ["DROP TABLE IF EXISTS regions", "CREATE TABLE regions (code VARCHAR(10), name VARCHAR(20))",
                  "INSERT INTO regions VALUES ('us', 'United States'), ('eu', 'Europe'), ('latam', 'Latin America')",
                  "SELECT s.id, r.name FROM sales s JOIN regions r ON r.code = s.region ORDER BY s.id",
                  "SELECT s.id, r.name FROM sales s LEFT JOIN regions r ON r.code = s.region ORDER BY s.id",
                  "SELECT s.id, r.code FROM sales s FULL OUTER JOIN regions r ON r.code = s.region ORDER BY s.id NULLS LAST, r.code"])


def test_subqueries_cte_union(pair):
    """Queries | subqueries (IN / EXISTS / scalar), WITH, UNION / UNION ALL"""
    pair(SALES + ["SELECT id FROM sales WHERE amount > (SELECT AVG(amount) FROM sales) ORDER BY id",
                  "SELECT id FROM sales s WHERE EXISTS (SELECT 1 FROM sales x WHERE x.region = s.region AND x.id <> s.id) ORDER BY id",
                  "WITH t AS (SELECT region, SUM(amount) AS total FROM sales GROUP BY region) SELECT region FROM t WHERE total > 10 ORDER BY region",
                  "SELECT region FROM sales WHERE id = 1 UNION SELECT region FROM sales WHERE id = 2",
                  "SELECT region FROM sales WHERE id < 3 UNION ALL SELECT region FROM sales WHERE id < 3 ORDER BY 1"])


def test_window_functions(pair):
    """Queries | ROW_NUMBER / RANK / DENSE_RANK / SUM OVER / LAG / LEAD / running total"""
    pair(SALES + ["SELECT id, ROW_NUMBER() OVER (PARTITION BY region ORDER BY amount DESC), "
                  "RANK() OVER (ORDER BY day), DENSE_RANK() OVER (ORDER BY day) FROM sales ORDER BY id",
                  "SELECT id, SUM(amount) OVER (PARTITION BY region), LAG(amount) OVER (ORDER BY id), "
                  "LEAD(id) OVER (ORDER BY id) FROM sales ORDER BY id",
                  "SELECT id, SUM(qty) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM sales ORDER BY id"])


def test_case_coalesce_cast(pair):
    """Queries | CASE, COALESCE, NULLIF, CAST and ::"""
    pair(SALES + ["SELECT id, CASE WHEN amount >= 10 THEN 'big' WHEN amount >= 4 THEN 'mid' ELSE 'small' END FROM sales ORDER BY id",
                  "SELECT id, COALESCE(product, 'none'), NULLIF(qty, 5) FROM sales ORDER BY id",
                  "SELECT CAST('42' AS INT), '3.5'::DECIMAL(5,2), CAST(amount AS VARCHAR) FROM sales WHERE id = 1"])


# ---- functions ----

def test_string_functions(pair):
    """Functions | UPPER / LOWER / LENGTH / SUBSTRING / TRIM / REPLACE / POSITION / SPLIT_PART / || / LPAD"""
    pair(["SELECT UPPER('ab'), LOWER('AB'), LENGTH('héllo'), SUBSTRING('hello', 2, 3), TRIM('  x  '), "
          "REPLACE('hello', 'l', 'L'), POSITION('l' IN 'hello'), SPLIT_PART('a,b,c', ',', 2), 'a' || 'b', LPAD('5', 3, '0')"])


def test_math_functions(pair):
    """Functions | ROUND / FLOOR / CEIL / ABS / MOD / POWER on numerics"""
    pair(["SELECT ROUND(2.567, 2), FLOOR(2.5), CEIL(2.1), ABS(-3), MOD(7, 3), POWER(2, 10)"])


def test_integer_division(pair):
    """Functions | integer / integer truncates (7 / 2 = 3, -7 / 2 = -3)"""
    pair(["SELECT 7 / 2, -7 / 2, 9 / 3"])


@pytest.mark.xfail(strict=True, reason="task 1: DECIMAL / INT is a float in DuckDB; Redshift and PostgreSQL return DECIMAL")
def test_decimal_division(pair):
    """Functions | DECIMAL division returns DECIMAL | task 1"""
    pair(["SELECT 7.0 / 2"])


def test_aggregates(pair):
    """Functions | COUNT / SUM / MIN / MAX on INT and DECIMAL columns"""
    pair(SALES + ["SELECT count(*), count(product), SUM(qty), SUM(amount), MIN(amount), MAX(day) FROM sales"])


@pytest.mark.xfail(strict=True, reason="task 1: AVG(DECIMAL) is a float in DuckDB; Redshift and PostgreSQL return DECIMAL")
def test_avg(pair):
    """Functions | AVG of a DECIMAL column | task 1"""
    pair(SALES + ["SELECT AVG(amount) FROM sales"])


def test_date_functions(pair):
    """Functions | DATE_TRUNC / date arithmetic / CURRENT_DATE"""
    pair(["SELECT DATE_TRUNC('month', TIMESTAMP '2026-03-04 05:06:07'), "
          "DATE '2026-03-04' + 1, DATE '2026-03-04' - DATE '2026-03-01', CURRENT_DATE = CURRENT_DATE"])


@pytest.mark.xfail(strict=True, reason="EXTRACT returns an integer in Redshift (and in s3shift); PostgreSQL 14+ returns numeric")
@pytest.mark.quirk
def test_extract_type(pair):
    """Functions | EXTRACT result type | Redshift / s3shift: integer, PostgreSQL: numeric"""
    pair(["SELECT EXTRACT(YEAR FROM DATE '2026-03-04'), EXTRACT(MONTH FROM TIMESTAMP '2026-03-04 05:06:07')"])


# ---- types ----

def test_result_types(pair):
    """Types | result types: int, Decimal (scale kept), float, str, date, datetime, bool, None"""
    pair(["SELECT 1, 1.50::DECIMAL(5,2), 2.5::DOUBLE PRECISION, 'x', DATE '2026-01-02', TIMESTAMP '2026-01-02 03:04:05', "
          "true, NULL::INT"])


def test_parameters(pair):
    """Types | parameters: int, str, Decimal, date, datetime, None"""
    pair([("SELECT %s, %s, %s, %s, %s, %s IS NULL",
           [1, "x", decimal.Decimal("1.25"), datetime.date(2026, 1, 2), datetime.datetime(2026, 1, 2, 3, 4, 5), None])])


# ---- errors ----

def test_errors(pair):
    """Errors | syntax error, missing table, missing column -> ProgrammingError; bad cast -> DataError"""
    pair(SALES + ["SELEC 1", "SELECT * FROM nope", "SELECT nope FROM sales", "SELECT CAST('abc' AS INT)"])


@pytest.mark.xfail(int(duckdb.__version__.split(".")[0]) < 2, strict=True,
                   reason="task 7: DuckDB before 2.0 returns NULL for x / 0 (the Lambda layer pins 2.0, which raises)")
def test_division_by_zero(pair):
    """Errors | division by zero raises (DuckDB 2.0+, as in the Lambda layer)"""
    pair(["SELECT 1 / 0"])
