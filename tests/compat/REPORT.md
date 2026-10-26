# s3shift compatibility report · [tasks](TASKS.md)

Generated 2026-10-26 by `pytest tests` · s3shift 0.1.0 · Python 3.9.6

Each row is one feature exercised the same way on the reference and on the project; results, types and errors must match. The feature list only grows (a removed case would show in git), so the support percentage can only move up.

Whole test run: **50 passed**, 0 failed, 4 expected failures (known gaps), 0 skipped — regression suite (`tests/`) and compatibility suite (`tests/compat/`) together.

Open tasks: task 1 (2), task 7 (1) — see [TASKS.md](TASKS.md).

## s3shift.redshift_connector vs PostgreSQL (Redshift's SQL dialect) — reference: PostgreSQL 16.15 (Debian 16.15-1.pgdg13+2) (Docker)

**Support 87%** (21 / 24 features) · 3 gap · 1 quirk · 20 verified

✅ verified · ❌ gap — known difference, the note names the task that closes it · ⚪ quirk — documented, deliberately not aligned · ⛔ unsupported by design, refused with NotSupportedError · 🟡 fixed — xfail marker to remove · 🔴 regression · ⏭ not run

| Feature | Status | Note |
|---|---|---|
| **DDL** | | |
| ALTER TABLE ADD COLUMN / DROP COLUMN | ✅ |  |
| CREATE TABLE ... AS SELECT | ✅ |  |
| CREATE TABLE with INT / BIGINT / DECIMAL / DOUBLE / VARCHAR / DATE / TIMESTAMP / BOOLEAN; DROP TABLE | ✅ |  |
| CREATE VIEW / CREATE OR REPLACE VIEW / DROP VIEW | ✅ |  |
| **DML** | | |
| INSERT (multi-row, with parameters) / UPDATE / DELETE and their rowcount | ✅ |  |
| INSERT INTO ... SELECT | ✅ |  |
| **Queries** | | |
| CASE, COALESCE, NULLIF, CAST and :: | ✅ |  |
| GROUP BY / HAVING, COUNT(DISTINCT), LIMIT / OFFSET | ✅ |  |
| INNER / LEFT / FULL OUTER JOIN | ✅ |  |
| ROW_NUMBER / RANK / DENSE_RANK / SUM OVER / LAG / LEAD / running total | ✅ |  |
| WHERE with IN / BETWEEN / LIKE / ILIKE / IS NULL; ORDER BY with NULLS FIRST / LAST | ✅ |  |
| subqueries (IN / EXISTS / scalar), WITH, UNION / UNION ALL | ✅ |  |
| **Functions** | | |
| COUNT / SUM / MIN / MAX on INT and DECIMAL columns | ✅ |  |
| DATE_TRUNC / date arithmetic / CURRENT_DATE | ✅ |  |
| ROUND / FLOOR / CEIL / ABS / MOD / POWER on numerics | ✅ |  |
| UPPER / LOWER / LENGTH / SUBSTRING / TRIM / REPLACE / POSITION / SPLIT_PART / || / LPAD | ✅ |  |
| integer / integer truncates (7 / 2 = 3, -7 / 2 = -3) | ✅ |  |
| AVG of a DECIMAL column | ❌ gap | task 1 |
| DECIMAL division returns DECIMAL | ❌ gap | task 1 |
| EXTRACT result type | ⚪ quirk | Redshift / s3shift: integer, PostgreSQL: numeric |
| **Types** | | |
| parameters: int, str, Decimal, date, datetime, None | ✅ |  |
| result types: int, Decimal (scale kept), float, str, date, datetime, bool, None | ✅ |  |
| **Errors** | | |
| syntax error, missing table, missing column -> ProgrammingError; bad cast -> DataError | ✅ |  |
| division by zero raises (DuckDB 2.0+, as in the Lambda layer) | ❌ gap |  |

## Redshift-only statements (checked against Redshift's documented behaviour) — reference: Redshift documentation

**Support 75%** (15 / 20 features) · 5 unsupported · 15 verified

✅ verified · ❌ gap — known difference, the note names the task that closes it · ⚪ quirk — documented, deliberately not aligned · ⛔ unsupported by design, refused with NotSupportedError · 🟡 fixed — xfail marker to remove · 🔴 regression · ⏭ not run

| Feature | Status | Note |
|---|---|---|
| **DDL** | | |
| DISTSTYLE / DISTKEY / SORTKEY / COMPOUND SORTKEY / ENCODE are accepted (layout is not needed here) | ✅ |  |
| VACUUM / ANALYZE / SET / BEGIN / COMMIT run as no-ops | ✅ |  |
| **Functions** | | |
| DATEADD / DATEDIFF with unquoted date parts | ✅ |  |
| GETDATE() / SYSDATE return a timestamp without fraction | ✅ |  |
| LISTAGG ... WITHIN GROUP (ORDER BY ...) | ✅ |  |
| MEDIAN / PERCENTILE_CONT ... WITHIN GROUP | ✅ |  |
| NVL / NVL2 / LEN | ✅ |  |
| TO_CHAR with YYYY MM DD HH24 MI SS | ✅ |  |
| **Load / unload** | | |
| COPY ... FORMAT AS PARQUET and FORMAT AS JSON 'auto' | ✅ |  |
| COPY FROM 's3://...' CSV IGNOREHEADER 1 IAM_ROLE '...' | ✅ |  |
| COPY without CSV reads pipe-delimited text (Redshift's default) | ✅ |  |
| UNLOAD ('...') TO 's3://...' FORMAT AS PARQUET / CSV HEADER, ALLOWOVERWRITE | ✅ |  |
| **External data** | | |
| CREATE EXTERNAL SCHEMA ... FROM POSTGRES: federated query joined with S3 data | ✅ |  |
| CREATE EXTERNAL TABLE ... ROW FORMAT DELIMITED ... STORED AS TEXTFILE, skip.header.line.count | ✅ |  |
| CREATE EXTERNAL TABLE ... STORED AS PARQUET LOCATION over many files (Spectrum) | ✅ |  |
| **Unsupported** | | |
| CREATE EXTERNAL SCHEMA FROM DATA CATALOG | ⛔ | declare external tables directly |
| CREATE FUNCTION (SQL / Python UDF) | ⛔ | server-side code |
| CREATE MATERIALIZED VIEW | ⛔ | use CREATE TABLE AS, refreshed by a schedule |
| CREATE PROCEDURE / CALL | ⛔ | server-side code; use application code or lambdaflow |
| GRANT / CREATE USER | ⛔ | the endpoint credentials are the only users |
