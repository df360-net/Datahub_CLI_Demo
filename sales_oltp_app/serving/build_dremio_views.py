"""Build the Sales serving layer in Dremio from serving/dremio_views.sql — idempotently, via REST.

Logs in, ensures the `Sales` space exists, then runs each CREATE OR REPLACE VIEW statement (polling
the async job to completion). Re-runnable: CREATE OR REPLACE makes every view a no-op-safe upsert, so
this is a natural daily Airflow task after the gold load. Reads DREMIO_ENDPOINT/USER/PWD from .env
(repo root or sales_oltp_app). See docs/Sales_Serving_Design.md.

Run:  sales_oltp_app/.venv/Scripts/python.exe serving/build_dremio_views.py
"""
import json
import os
import time
import urllib.error
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
SQL_FILE = os.path.join(HERE, "dremio_views.sql")
SPACE = "sales_curated"


def load_env():
    env = {}
    for rel in ("../../.env", "../.env"):  # repo root first, then sales_oltp_app
        path = os.path.normpath(os.path.join(HERE, rel))
        if os.path.exists(path):
            for line in open(path, encoding="utf-8"):
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    k, v = line.split("=", 1)
                    env.setdefault(k.strip(), v.strip())
    return env


ENV = load_env()
EP = ENV["DREMIO_ENDPOINT"].rstrip("/")


def call(path, body=None, token=None, base="/api/v3", method=None):
    url = EP + base + path
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method or ("POST" if data else "GET"),
                                 headers={"Content-Type": "application/json"})
    if token:
        req.add_header("Authorization", f"_dremio{token}")
    with urllib.request.urlopen(req, timeout=60) as r:
        return json.loads(r.read())


def login():
    return call("/login", {"userName": ENV["DREMIO_USER"], "password": ENV["DREMIO_PWD"]},
                base="/apiv2")["token"]


def ensure_space(token):
    top = call("/catalog", token=token)["data"]
    if any(it.get("path") == [SPACE] for it in top):
        print(f"  space {SPACE!r} exists")
        return
    call("/catalog", {"entityType": "space", "name": SPACE}, token=token)
    print(f"  space {SPACE!r} created")


def run_sql(token, sql):
    jid = call("/sql", {"sql": sql}, token=token)["id"]
    while True:
        job = call(f"/job/{jid}", token=token)
        st = job["jobState"]
        if st in ("COMPLETED", "FAILED", "CANCELED"):
            return st, job.get("errorMessage", "")
        time.sleep(0.4)


def statements():
    raw = open(SQL_FILE, encoding="utf-8").read()
    lines = [ln for ln in raw.splitlines() if not ln.strip().startswith("--")]
    for stmt in " ".join(lines).split(";"):
        stmt = stmt.strip()
        if stmt:
            yield stmt


def view_name(stmt):
    # ...VIEW <name> AS... -> <name>
    toks = stmt.split()
    return toks[toks.index("VIEW") + 1] if "VIEW" in toks else stmt[:40]


def main():
    token = login()
    print(f"connected to Dremio @ {EP}")
    ensure_space(token)
    ok = 0
    for stmt in statements():
        st, err = run_sql(token, stmt)
        name = view_name(stmt)
        if st == "COMPLETED":
            ok += 1
            print(f"  [OK]   {name}")
        else:
            print(f"  [{st}] {name}\n         {err[:400]}")
    print(f"serving build complete: {ok} views applied to space {SPACE!r}")


if __name__ == "__main__":
    main()
