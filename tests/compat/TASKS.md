# Tasks

The numbers [REPORT.md](REPORT.md) refers to in its gap notes. Open first; done ones stay below so old
notes keep resolving. Numbers are never reused.

## Open

| task | what |
|---|---|
| 1 | DECIMAL arithmetic keeps DECIMAL: `AVG(decimal)` and `decimal / int` are floats in DuckDB, DECIMAL in Redshift |
| 2 | CloudFormation template (the layer must be built before `aws cloudformation package`) |
| 3 | `CREATE EXTERNAL SCHEMA ... SECRET_ARN`: read the RDS credentials from Secrets Manager |
| 4 | Redshift functions still missing: `RATIO_TO_REPORT`, `DECODE`, `CONVERT_TIMEZONE`, `APPROXIMATE COUNT(DISTINCT)`, `JSON_EXTRACT_PATH_TEXT` |
| 5 | /tmp cache eviction: drop the least recently used S3 files when /tmp runs low |
| 6 | Partition pruning for external tables (`LOCATION .../day=2026-01-01/`): read only the partitions a WHERE clause needs |

Boundaries, not tasks: one query = one Lambda (up to 10 GB memory, 15 minutes, 10 GB /tmp); no JDBC /
PostgreSQL wire protocol (BI tools in Java cannot connect; Python ones use the driver); managed tables are one
Parquet object rewritten on every write (fine for reporting tables, not for streaming inserts).
⛔ unsupported: stored procedures, UDFs, users / GRANT, Glue Data Catalog schemas, materialized views.

## Done

| task | what | release |
|---|---|---|
| 7 | `x / 0` raises: DuckDB 2.0 does, the layer pins `duckdb==2.0.0` (local runs on DuckDB 1.x still show it as a gap) | 0.1.0 |
