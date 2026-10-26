"""s3shift — a truly serverless Redshift: Redshift SQL on DuckDB in a Lambda, over your own S3 and RDS data.

    from s3shift import redshift_connector    # later: `import redshift_connector` against Redshift
    conn = redshift_connector.connect(host="xxx.lambda-url.us-east-2.on.aws", database="dev",
                                      user="admin", password="...")
"""
from . import redshift_connector  # noqa: F401

__version__ = "0.1.0"
