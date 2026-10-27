# s3shift

**Python client for [s3shift](https://github.com/cloudformer/s3shift) — a truly serverless Redshift: Redshift
SQL on DuckDB in a Lambda, over your own S3 and RDS data, ≈ $0 when idle.** The redshift_connector API,
standard library only. Part of [Redefine AWS Serverless](https://github.com/topics/redefine-aws-serverless).

![s3shift architecture](https://raw.githubusercontent.com/cloudformer/s3shift/main/docs/architecture.svg)

```bash
pip install s3shift
```

```python
from s3shift import redshift_connector   # later: `import redshift_connector` against Redshift, nothing else changes

conn = redshift_connector.connect(host="xxx.lambda-url.us-east-2.on.aws", database="dev",
                                  user="admin", password="...")
cur = conn.cursor()
cur.execute("CREATE EXTERNAL TABLE clicks (user_id INT, day DATE) STORED AS PARQUET LOCATION 's3://my-lake/clicks/'")
cur.execute("SELECT day, count(*) FROM clicks WHERE day >= %s GROUP BY 1 ORDER BY 1", ["2026-01-01"])
cur.fetchall()            # ([datetime.date(2026, 1, 1), 1520], ...)
cur.fetch_dataframe()     # pandas, if installed
```

Every statement is committed when `execute()` returns; large results are fetched page by page inside
`execute()`. Not supported (raises `NotSupportedError`): stored procedures, UDFs, users / GRANT, Glue
Data Catalog schemas, materialized views.

The endpoint comes from deploying the [main repository](https://github.com/cloudformer/s3shift).
Compatibility: [REPORT.md](https://github.com/cloudformer/s3shift/blob/main/tests/compat/REPORT.md).
