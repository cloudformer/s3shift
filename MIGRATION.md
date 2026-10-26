# Moving to Redshift

s3shift is for analytics that run in bursts: reports, ad-hoc questions, scheduled ETL over data you already
keep in S3 and RDS. When you outgrow it, Redshift is one import line away — your data never moved.

**When:** single queries need more than 10 GB of memory or 15 minutes (TB-scale joins), BI tools must
connect over JDBC, or many dashboards refresh around the clock (then a warehouse that is always up costs less).

1. Create Redshift Serverless (or a provisioned cluster) in the same region.
2. Point it at the same data: the external tables you created become Spectrum external tables
   (`CREATE EXTERNAL SCHEMA ... FROM DATA CATALOG` + the same `CREATE EXTERNAL TABLE` statements); RDS
   schemas become federated schemas (`CREATE EXTERNAL SCHEMA ... FROM POSTGRES | MYSQL ... SECRET_ARN`).
   Managed tables: `UNLOAD` from s3shift is not needed — `COPY t FROM 's3://<bucket>/dev/tables/t/' FORMAT AS PARQUET`
   loads the Parquet object s3shift already wrote.
3. Change the import: `from s3shift import redshift_connector` → `import redshift_connector`;
   `connect(host=, database=, user=, password=)` keeps the same shape (add `port=5439`).
4. Delete the stack: `terraform -chdir=terraform destroy -var-file=my.json`.

What you gain: TB-scale queries, JDBC / BI tools, workload management. What you lose: the ≈ $0 idle bill.
