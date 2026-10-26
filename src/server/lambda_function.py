"""s3shift server: one Lambda behind a Function URL — Redshift SQL over HTTP for the driver, a Query Editor
style console at GET /. The engine (engine.py) is DuckDB over your S3 and RDS data.

    GET  /                         console (index.html)
    POST /api/query                {"sql", "params"?, "database"?} -> {columns, rows, rowcount, scanned, ms}
                                   or {"next_token"} for the next page of a large result
    GET  /api/tables?database=     console: the catalog (tables, views, external schemas)

Large results: replies are capped at PAGE_BYTES; the rest is written to s3://<bucket>/results/<id>.json
(deleted by a lifecycle rule after a day) and served page by page with next_token.

Values that JSON cannot carry travel as markers: {"$n": "1.50"} Decimal, {"$d": "2026-01-02"} date,
{"$ts": "2026-01-02 03:04:05"} timestamp, {"$t": "03:04:05"} time, {"$b": base64} bytes.
"""
import base64
import datetime
import decimal
import hmac
import json
import os
import re
import time
import uuid

import boto3

from engine import Engine, SQLError

USER = os.environ.get("USER_NAME", "")
PASSWORD = os.environ.get("PASSWORD", "")
CONSOLE = os.environ.get("CONSOLE", "1") == "1"
BUCKET = os.environ.get("BUCKET", "s3shift")
PAGE_BYTES = 4 * 1024 * 1024   # well under Lambda's 6 MB response limit

with open(os.path.join(os.path.dirname(__file__), "index.html"), encoding="utf-8") as f:
    INDEX_HTML = f.read()

_s3 = None


def s3():
    global _s3
    if _s3 is None:
        _s3 = boto3.client("s3")
    return _s3


class BadRequest(Exception):
    pass


# ---------------- shared: the same in every project ----------------

def _json(status, data):
    return {"statusCode": status, "headers": {"content-type": "application/json"},
            "body": json.dumps(data, ensure_ascii=False, default=str)}


def _body(event):
    raw = event.get("body") or "{}"
    try:
        if event.get("isBase64Encoded"):
            raw = base64.b64decode(raw)
        data = json.loads(raw)
    except ValueError:
        raise BadRequest("request body is not valid JSON") from None
    if not isinstance(data, dict):
        raise BadRequest("request body must be a JSON object")
    return data


def _authed(event):
    if not USER:
        return True
    auth = (event.get("headers") or {}).get("authorization", "")
    if not auth.lower().startswith("basic "):
        return False
    try:
        user, _, password = base64.b64decode(auth.split(None, 1)[1]).decode().partition(":")
    except Exception:
        return False
    return hmac.compare_digest(user.encode(), USER.encode()) & hmac.compare_digest(password.encode(), PASSWORD.encode())


def lambda_handler(event, context):
    http = (event.get("requestContext") or {}).get("http") or {}
    method, path = http.get("method", "GET"), event.get("rawPath", "/")
    if CONSOLE and method == "GET" and path in ("/", "/index.html"):
        return {"statusCode": 200, "headers": {"content-type": "text/html; charset=utf-8"}, "body": INDEX_HTML}
    if not _authed(event):
        return _json(401, {"error": "bad credentials"})
    t0 = time.perf_counter()
    status, data = 404, {"error": f"no route: {method} {path}"}
    try:
        for route_method, pattern, handler in ROUTES:
            match = pattern.fullmatch(path)
            if match and route_method == method:
                out = handler(event, **match.groupdict())
                status, data = out if isinstance(out, tuple) else (200, out)
                break
    except BadRequest as e:
        status, data = 400, {"error": str(e)}
    except SQLError as e:
        status, data = 400, {"error": str(e), "kind": e.kind}
    except Exception as e:  # noqa: BLE001
        status, data = 500, {"error": f"{type(e).__name__}: {e}"}
    data.setdefault("ms", round((time.perf_counter() - t0) * 1000, 1))
    return _json(status, data)


# ---------------- values ----------------

def _cell(v):
    if isinstance(v, decimal.Decimal):
        return {"$n": str(v)}
    if isinstance(v, datetime.datetime):
        return {"$ts": v.isoformat(sep=" ")}
    if isinstance(v, datetime.date):
        return {"$d": v.isoformat()}
    if isinstance(v, datetime.time):
        return {"$t": v.isoformat()}
    if isinstance(v, (bytes, bytearray, memoryview)):
        return {"$b": base64.b64encode(bytes(v)).decode()}
    if isinstance(v, float) and (v != v or v in (float("inf"), float("-inf"))):
        return {"$f": str(v)}
    if isinstance(v, (datetime.timedelta, uuid.UUID)):
        return str(v)
    return v


def _param(v):
    if isinstance(v, dict) and len(v) == 1:
        (tag, text), = v.items()
        if tag == "$b":
            return base64.b64decode(text)
        if tag == "$n":
            return decimal.Decimal(text)
        if tag == "$d":
            return datetime.date.fromisoformat(text)
        if tag == "$ts":
            return datetime.datetime.fromisoformat(text)
    return v


def _page(rows, offset):
    """As many rows from offset as fit in PAGE_BYTES."""
    out, size = [], 0
    for row in rows[offset:]:
        size += len(json.dumps(row, default=str)) + 2
        if out and size > PAGE_BYTES:
            break
        out.append(row)
    return out


# ---------------- the API ----------------

def query(event):
    body = _body(event)
    token = body.get("next_token")
    if token:
        try:
            rid, offset = token.rsplit(":", 1)
            offset = int(offset)
            stored = json.loads(s3().get_object(Bucket=BUCKET, Key=f"results/{rid}.json")["Body"].read())
        except Exception:
            raise BadRequest("invalid or expired next_token") from None
        rows = _page(stored["rows"], offset)
        nxt = offset + len(rows)
        out = {"columns": stored["columns"], "rows": rows, "rowcount": stored["rowcount"]}
        if nxt < len(stored["rows"]):
            out["next_token"] = f"{rid}:{nxt}"
        return out
    sql = body.get("sql")
    if not isinstance(sql, str) or not sql.strip():
        raise BadRequest("sql must be a non-empty string")
    params = body.get("params") or []
    if not isinstance(params, list):
        raise BadRequest("params must be a list")
    engine = Engine(BUCKET, body.get("database") or "dev", s3())
    result = engine.run(sql, [_param(p) for p in params])
    rows = [[_cell(v) for v in r] for r in result["rows"]]
    out = {"columns": result["columns"], "rows": rows, "rowcount": result["rowcount"], "scanned": engine.scanned}
    first = _page(rows, 0)
    if len(first) < len(rows):
        rid = uuid.uuid4().hex
        s3().put_object(Bucket=BUCKET, Key=f"results/{rid}.json",
                        Body=json.dumps({"columns": result["columns"], "rows": rows, "rowcount": result["rowcount"]}, default=str).encode())
        out["rows"], out["next_token"] = first, f"{rid}:{len(first)}"
    return out


def tables(event):
    qs = event.get("queryStringParameters") or {}
    cat = Engine(BUCKET, qs.get("database") or "dev", s3()).catalog()
    schemas = {n: {k: v for k, v in s.items() if k != "password"} for n, s in cat["schemas"].items()}
    return {"tables": cat["tables"], "schemas": schemas}


ROUTES = [
    ("POST", re.compile(r"/api/query$"), query),
    ("GET",  re.compile(r"/api/tables$"), tables),
]
