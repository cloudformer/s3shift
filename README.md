# s3shift — a truly serverless Redshift

> **Redshift Serverless** starts billing the moment a query wakes it (8 RPU minimum ≈ $3 an hour, 60-second
> minimum per query). **s3shift** runs Redshift SQL on **DuckDB inside a Lambda, directly over your S3 and RDS
> data**, and costs ≈ $0 when nobody queries. Same client API (redshift_connector); when you outgrow it,
> go back to Redshift by changing one import line — your data never moved.

[中文](README.zh.md) · [compatibility report](tests/compat/REPORT.md) · [tasks](tests/compat/TASKS.md) · [changelog](CHANGELOG.md)

![architecture](docs/architecture.svg)

| | Redshift Serverless | s3shift |
|---|---|---|
| Idle cost / month | $0 idle, but ≈ $3 / hour (8 RPU) once a query runs, 60 s minimum each | ≈ $0; a query costs Lambda seconds + S3 reads |
| Endpoint | host:5439, PostgreSQL protocol, VPC | Lambda Function URL, HTTPS, anywhere |
| Data | loaded into the warehouse, or Spectrum over S3 | read in place: S3 files and RDS tables, nothing loaded |
| Concurrency | queries share the RPUs and queue | one Lambda per query: many analysts never wait for each other |
| Largest query | TB scale | one Lambda: up to 10 GB memory, 15 minutes |
| Client | `import redshift_connector` | `from s3shift import redshift_connector` |
| Console | Query Editor v2 | built-in query editor (Lambda URL) |
| Compatibility | — | [see the report](tests/compat/REPORT.md) |

## Why

Most teams use Redshift for daily reports, ad-hoc questions and scheduled ETL over data that already sits in
S3 and RDS. Those queries arrive in bursts, and a warehouse — even a serverless one — charges by the hour of
compute while they run. DuckDB answers the same SQL over the same files in a single process; put it in a
Lambda and every query gets its own engine, billed by the millisecond, with nothing to keep warm.

## How it works

Your code (or the browser console) sends SQL to the Lambda Function URL with HTTP Basic auth. The Lambda
opens an in-memory DuckDB, looks up the tables the statement mentions in a small catalog (one JSON object in
S3), and makes them visible: external tables read your Parquet / CSV / JSON files from S3 (cached in /tmp by
ETag), RDS tables are read through the standard client (pymysql / psycopg2), managed tables are Parquet
objects s3shift writes itself. Redshift spellings (`DISTKEY`, `NVL`, `GETDATE()`, `DATEADD(day, …)`,
`LISTAGG … WITHIN GROUP`, `COPY`, `UNLOAD`) are translated or handled before DuckDB runs. When the query
returns, the engine is gone.

## Quick start

### 1. Deploy

```bash
cp terraform/example.json terraform/my.json      # set aws_profile, env, user, password, read_buckets
terraform -chdir=terraform init
terraform -chdir=terraform apply -var-file=my.json
terraform -chdir=terraform output                 # console_url, bucket
terraform -chdir=terraform destroy -var-file=my.json   # tear everything down (managed tables included)
```

`apply` downloads DuckDB and the RDS clients for Lambda's Linux with `pip` (needs `python3` on your machine)
and publishes them as a layer. It creates one S3 bucket (catalog, managed tables, large results kept for a
day), one Lambda with a public Function URL (3 GB memory, 15-minute timeout, 10 GB /tmp), and one IAM role
that can read the buckets you list in `read_buckets`. To query a private RDS, set `vpc_subnet_ids` /
`vpc_security_group_ids` (the Lambda then needs an S3 gateway endpoint). CloudFormation: planned (task 2).

### 2. Open the query editor

Open `console_url` in a browser, sign in with `user` / `password`. Paste this to get data to click around
(the statements run in order; replace the bucket in the last two with one from `read_buckets` to try your own data):

```sql
CREATE TABLE sales (id INT, region VARCHAR(20), product VARCHAR(20), amount DECIMAL(10,2), day DATE) DISTKEY(id) SORTKEY(day);
INSERT INTO sales VALUES (1, 'us', 'apple', 10.50, '2026-01-01'), (2, 'us', 'pear', 4.00, '2026-01-02'),
  (3, 'eu', 'apple', 7.25, '2026-01-02'), (4, 'eu', 'kiwi', 1.00, '2026-02-01'), (5, 'asia', 'kiwi', 12.00, '2026-02-03');
SELECT region, SUM(amount) AS total, LISTAGG(product, ',') WITHIN GROUP (ORDER BY product) AS products
  FROM sales GROUP BY region ORDER BY total DESC;
SELECT TO_CHAR(day, 'YYYY-MM') AS month, COUNT(*), SUM(amount) FROM sales GROUP BY 1 ORDER BY 1;
-- CREATE EXTERNAL TABLE events (user_id INT, kind VARCHAR) STORED AS PARQUET LOCATION 's3://my-lake/events/';
-- SELECT kind, COUNT(*) FROM events GROUP BY kind;
```

The left side lists tables, external tables, views and RDS schemas; click one to preview it. Every query shows
the server time and the bytes it read from S3; results export as CSV.

### 3. Connect from code

```bash
pip install s3shift
```

```python
from s3shift import redshift_connector   # later: `import redshift_connector` against Redshift, nothing else changes

conn = redshift_connector.connect(host="xxx.lambda-url.us-east-2.on.aws", database="dev",
                                  user="admin", password="...")
cur = conn.cursor()
cur.execute("CREATE EXTERNAL SCHEMA app FROM POSTGRES DATABASE 'shop' URI 'db.xxx.rds.amazonaws.com' "
            "USER 'reader' PASSWORD '...'")
cur.execute("""SELECT c.country, SUM(o.amount) FROM orders o JOIN app.customers c ON c.id = o.customer_id
               WHERE o.day >= %s GROUP BY 1 ORDER BY 2 DESC""", ["2026-01-01"])
cur.fetchall()                 # ([country, Decimal(...)], ...)
cur.fetch_dataframe()          # pandas, if installed
cur.execute("UNLOAD ('SELECT * FROM orders WHERE day >= ''2026-01-01''') TO 's3://my-lake/exports/orders_' FORMAT AS PARQUET")
```

## Compatibility

[tests/compat/REPORT.md](tests/compat/REPORT.md) is regenerated by every test run. The SQL Redshift inherited from
PostgreSQL runs on a real PostgreSQL 16 (Docker) and on s3shift, compared row by row with Python types;
Redshift-only statements (`COPY`, `UNLOAD`, external tables and schemas, `DISTKEY`, `NVL`, …) are checked
against Redshift's documented behaviour.

| Status | Meaning |
|---|---|
| ✅ verified | identical to the reference; safe to rely on |
| ❌ gap | known difference; the note names the task in [TASKS.md](tests/compat/TASKS.md) that closes it |
| ⚪ quirk | where PostgreSQL and Redshift differ, s3shift follows Redshift |
| ⛔ unsupported | refused with `NotSupportedError` — never silently different |

Run it yourself — the same commands CI runs on every push:

```bash
python3 -m pip install pytest boto3 moto duckdb psycopg2-binary
docker compose -f tests/compat/docker-compose.yml up -d postgres
POSTGRES_DSN=postgresql://postgres:test@127.0.0.1:5433/compat python3 -m pytest tests -q
docker compose -f tests/compat/docker-compose.yml down
COMPAT_DRY=1 python3 -m pytest tests/compat -q -s        # no Docker: s3shift side only
```

**Not supported** — raises `NotSupportedError`: stored procedures, UDFs, users / GRANT, Glue Data Catalog
schemas, materialized views.

## Guarantees

| | How |
|---|---|
| **Your data stays yours** | S3 files and RDS tables are read in place; s3shift writes only to its own bucket (and where you `UNLOAD`) |
| **Atomic writes** | a write to a managed table replaces its Parquet object in one PUT, guarded by If-Match: a concurrent writer gets an error, never a lost update |
| **Consistent catalog** | `catalog.json` is updated with If-Match as well |
| **Complete results** | results over 4 MB are stored for a day and fetched page by page inside `execute()`; nothing is truncated |
| **No duplicate writes** | the driver re-sends only when the request provably never reached the server |
| **Availability** | Lambda and S3 are regional, multi-AZ: no cluster to fail, resize or pause |

## Who this is for

Reporting, ad-hoc analysis, scheduled ETL and log analysis over data in S3 and RDS — from a few MB to hundreds
of GB if it is partitioned — by Python code, notebooks and Python dashboards (Streamlit, Dash). Many analysts at
once is fine: each query gets its own Lambda. Two boundaries: one query must fit one Lambda (10 GB memory,
15 minutes), and there is no JDBC / PostgreSQL protocol, so Java BI tools (QuickSight, Tableau, Metabase)
cannot connect. Hit either, and that is the moment to move to Redshift — see [MIGRATION.md](MIGRATION.md).

## Repository layout

```
src/server/       the service: lambda_function.py (HTTP API + console), engine.py (Redshift SQL on DuckDB), index.html
client/           the pip package s3shift: s3shift/redshift_connector.py, pyproject.toml
terraform/        deployment (the layer with DuckDB is built by pip during apply)
tests/            regression suite (moto); tests/compat/ compatibility suite, REPORT.md, TASKS.md
```

## Releasing

CI runs the whole suite, PostgreSQL included, on every push. A release is a tag: bump the version in
`client/pyproject.toml` and `client/s3shift/__init__.py`, add the CHANGELOG entry, commit, then
`git tag vX.Y.Z && git push --tags` (tests → build → PyPI; the tag must equal the version).

---

Part of **[Redefine AWS Serverless](https://github.com/topics/redefine-aws-serverless)** — AWS services that bill
while idle, rebuilt on primitives that cost ≈ $0 when idle. MIT.
