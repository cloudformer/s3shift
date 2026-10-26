"""Compatibility harness: the same SQL on a real PostgreSQL (Docker; Redshift's dialect comes from it) and on
s3shift (in-process Lambda, mocked S3), compared row by row with Python types. Errors are compared by
their DB-API class (ProgrammingError, DataError, ...). One test = one feature; its docstring is the report row.
"""
import os
from dataclasses import dataclass

import pytest

import compat_report

_DBAPI = ("IntegrityError", "DataError", "ProgrammingError", "OperationalError", "NotSupportedError", "InternalError")


@dataclass
class Outcome:
    rows: object          # list of lists for statements that return rows, else None
    rowcount: object
    error: object = None  # DB-API class name


def _dbapi_class(e):
    for cls in type(e).__mro__:
        if cls.__name__ in _DBAPI:
            return cls.__name__
    return type(e).__name__


def run_step(conn, sql, params):
    cur = conn.cursor()
    try:
        cur.execute(sql, params or None)
        rows = [list(r) for r in cur.fetchall()] if cur.description else None
        return Outcome(rows, cur.rowcount if rows is None else len(rows))
    except Exception as e:  # noqa: BLE001
        if hasattr(conn, "rollback") and not hasattr(conn, "_post"):
            conn.rollback()
        return Outcome(None, None, _dbapi_class(e))


def typed(rows):
    return None if rows is None else [[(type(v).__name__, v) for v in r] for r in rows]


@pytest.fixture
def real():
    dsn = os.environ.get("POSTGRES_DSN")
    if not dsn and os.environ.get("COMPAT_DRY"):
        yield None          # dry run: s3shift side only
        return
    if not dsn:
        pytest.skip("set POSTGRES_DSN (see tests/compat/docker-compose.yml) to run against a real PostgreSQL")
    psycopg2 = pytest.importorskip("psycopg2")
    c = psycopg2.connect(dsn)
    c.autocommit = True
    c.cursor().execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public")   # every test starts empty, like ours
    if not compat_report.versions.get("reference"):
        cur = c.cursor()
        cur.execute("SHOW server_version")
        compat_report.versions["reference"] = f"PostgreSQL {cur.fetchone()[0]} (Docker)"
    yield c
    c.close()


@pytest.fixture
def pair(real, conn):
    dry = real is None

    def both(sql, params=None):
        o = run_step(conn, sql, params)
        if dry:
            print(f"  {'ERR ' + str(o.error) if o.error else 'ok  ' + repr(o.rows if o.rows is not None else o.rowcount)[:70]:80s} {sql[:70]}")
            return o, o
        return run_step(real, sql, params), o

    def run(steps):
        for step in steps:
            sql, params = (step, None) if isinstance(step, str) else (step[0], step[1])
            r, o = both(sql, params)
            if dry:
                continue
            if r.error or o.error:
                assert r.error == o.error, f"{sql}\n  real: {r.error}\n  ours: {o.error}"
            elif r.rows is not None:
                assert typed(r.rows) == typed(o.rows), f"{sql}\n  real: {typed(r.rows)}\n  ours: {typed(o.rows)}"
            else:
                assert r.rowcount == o.rowcount, f"{sql}\n  rowcount real={r.rowcount} ours={o.rowcount}"
    run.both = both
    run.dry = dry
    return run
