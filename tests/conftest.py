"""Test setup: src/server and client/ on the import path, a mocked S3 bucket (moto), the report hooks.

The report hooks must live in the ROOT conftest so they see the whole run (regression + compat).
"""
import base64
import json
import os
import re
import sys
import tempfile

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src", "server"))
sys.path.insert(0, os.path.join(ROOT, "client"))

with open(os.path.join(ROOT, "client", "pyproject.toml"), encoding="utf-8") as _f:
    PACKAGE = re.search(r'^name\s*=\s*"([^"]+)"', _f.read(), re.M).group(1)
os.environ["PACKAGE"] = PACKAGE

# lambda_function / engine / boto3 read these at import time
os.environ.setdefault("USER_NAME", "admin")
os.environ.setdefault("PASSWORD", "pw")
os.environ.setdefault("BUCKET", "s3shift-test")
os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-2")
os.environ.setdefault("AWS_ACCESS_KEY_ID", "testing")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "testing")
os.environ.setdefault("S3SHIFT_CACHE", tempfile.mkdtemp(prefix="s3shift-cache-"))

from compat_report import (pytest_collection_modifyitems, pytest_configure,  # noqa: E402,F401
                           pytest_runtest_logreport, pytest_sessionfinish, pytest_terminal_summary)

AUTH = "Basic " + base64.b64encode(b"admin:pw").decode()


@pytest.fixture
def bucket():
    """A fresh mocked S3 bucket per test, wired into the server."""
    import boto3
    from moto import mock_aws

    import lambda_function as lf

    with mock_aws():
        client = boto3.client("s3")
        client.create_bucket(Bucket=os.environ["BUCKET"], CreateBucketConfiguration={"LocationConstraint": "us-east-2"})
        lf._s3 = client
        yield client
        lf._s3 = None


@pytest.fixture
def conn(bucket, monkeypatch):
    """s3shift.redshift_connector with its HTTP layer calling the Lambda handler in-process."""
    import lambda_function as lf
    from s3shift import redshift_connector

    c = redshift_connector.connect(host="in-process.test", database="dev", user="admin", password="pw")

    def post(path, payload):
        event = {"requestContext": {"http": {"method": "POST"}}, "rawPath": path,
                 "headers": {"authorization": AUTH}, "body": json.dumps(payload)}
        resp = lf.lambda_handler(event, None)
        data = json.loads(resp["body"])
        if resp["statusCode"] == 401:
            raise redshift_connector.InterfaceError("authentication failed")
        if resp["statusCode"] >= 400:
            raise redshift_connector._ERRORS.get(data.get("kind"), redshift_connector.DatabaseError)(data.get("error"))
        return data

    monkeypatch.setattr(c, "_post", post)
    return c
