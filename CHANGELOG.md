# Changelog

## 0.1.0 — 2026-10-26

First release. `from s3shift import redshift_connector`: Redshift SQL on DuckDB in a Lambda, over your own
S3 and RDS data, ≈ $0 when idle.

- Tables: `CREATE TABLE` / `CREATE TABLE AS` / `INSERT` / `UPDATE` / `DELETE` / `ALTER TABLE` / `DROP` on managed
  tables (one Parquet object each, replaced with If-Match), `CREATE VIEW`.
- External data: `CREATE EXTERNAL TABLE ... STORED AS PARQUET | TEXTFILE | JSON LOCATION 's3://...'` over your files;
  `CREATE EXTERNAL SCHEMA ... FROM POSTGRES | MYSQL` reads RDS tables through the standard client.
- `COPY ... FROM 's3://...'` (CSV, pipe-delimited, Parquet, JSON) and `UNLOAD (...) TO 's3://...'` (Parquet, CSV).
- Redshift spellings: `DISTKEY` / `SORTKEY` / `DISTSTYLE` / `ENCODE` accepted, `NVL`, `NVL2`, `LEN`, `GETDATE()`,
  `SYSDATE`, `DATEADD` / `DATEDIFF`, `TO_CHAR`, `LISTAGG ... WITHIN GROUP`, `MEDIAN`, integer division.
- Driver: DB-API cursors, `%s` / `%(name)s` parameters, `fetch_dataframe()`, typed results (Decimal, date, datetime),
  complete results over pages; redshift_connector's exception classes.
- Query editor console: tables / external tables / views / external schemas, SQL with `;`-separated statements,
  results with time and bytes scanned, CSV export.
