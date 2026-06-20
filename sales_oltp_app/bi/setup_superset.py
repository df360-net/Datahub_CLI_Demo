"""Provision the Superset BI layer from code — Dremio connection + vw_sales_report dataset + metrics.

Idempotent (get-or-create / upsert), via the Superset REST API (JWT + CSRF). Re-runnable. This is the
"dashboards as code" entry point: it stands up everything a dashboard needs, so the BI catalog is
reproducible from git rather than hand-clicked. See docs/Sales_BI_Design.md.

Reads DREMIO_USER/PWD from .env (repo root or sales_oltp_app); Superset URL/creds from env with lab
defaults. Run:  sales_oltp_app/.venv/Scripts/python.exe bi/setup_superset.py
"""
import http.cookiejar
import json
import os
import urllib.error
import urllib.request
from urllib.parse import quote

HERE = os.path.dirname(os.path.abspath(__file__))

DB_NAME = "Dremio (sales_curated)"
SCHEMA = "sales_curated"
TABLE = "vw_sales_report"

METRICS = [
    {"metric_name": "revenue", "expression": "SUM(extended_amount)", "metric_type": "sum", "verbose_name": "Revenue", "d3format": "$,.2f"},
    {"metric_name": "units", "expression": "SUM(quantity)", "metric_type": "sum", "verbose_name": "Units Sold"},
    {"metric_name": "orders", "expression": "COUNT(DISTINCT order_id)", "metric_type": "count_distinct", "verbose_name": "Orders"},
    {"metric_name": "avg_line", "expression": "AVG(extended_amount)", "metric_type": "avg", "verbose_name": "Avg Line Amount", "d3format": "$,.2f"},
]


def load_env():
    env = {}
    for rel in ("../../.env", "../.env"):
        path = os.path.normpath(os.path.join(HERE, rel))
        if os.path.exists(path):
            for line in open(path, encoding="utf-8"):
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    k, v = line.split("=", 1)
                    env.setdefault(k.strip(), v.strip())
    return env


ENV = load_env()
SS_URL = os.environ.get("SUPERSET_URL", "http://192.168.0.21:8088").rstrip("/")
SS_USER = os.environ.get("SUPERSET_USER", "admin")
SS_PWD = os.environ.get("SUPERSET_PWD", "password")
DREMIO_USER = ENV.get("DREMIO_USER", "jianminwei")
DREMIO_PWD = ENV.get("DREMIO_PWD", "")
DREMIO_URI = f"dremio+flight://{DREMIO_USER}:{quote(DREMIO_PWD, safe='')}@dremio:32010/dremio?UseEncryption=false"

opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(http.cookiejar.CookieJar()))
_state = {"access": None, "csrf": None}


def api(method, path, body=None):
    url = SS_URL + path
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method, headers={"Content-Type": "application/json"})
    if _state["access"]:
        req.add_header("Authorization", f"Bearer {_state['access']}")
    if _state["csrf"] and method != "GET":
        req.add_header("X-CSRFToken", _state["csrf"])
        req.add_header("Referer", SS_URL)
    try:
        with opener.open(req, timeout=60) as r:
            return r.status, json.loads(r.read() or b"{}")
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read() or b"{}")


def login():
    st, r = api("POST", "/api/v1/security/login",
                {"username": SS_USER, "password": SS_PWD, "provider": "db", "refresh": True})
    if st != 200:
        raise SystemExit(f"login failed {st}: {r}")
    _state["access"] = r["access_token"]
    _, r = api("GET", "/api/v1/security/csrf_token/")
    _state["csrf"] = r.get("result")
    print(f"logged in to {SS_URL} as {SS_USER}")


def get_database():
    _, r = api("GET", "/api/v1/database/?q=(page_size:100)")
    for d in r.get("result", []):
        if d["database_name"] == DB_NAME:
            return d["id"]
    return None


def ensure_database():
    db_id = get_database()
    if db_id:
        print(f"  database exists: {DB_NAME} (id={db_id})")
        return db_id
    st, r = api("POST", "/api/v1/database/", {
        "database_name": DB_NAME,
        "sqlalchemy_uri": DREMIO_URI,
        "expose_in_sqllab": True,
    })
    if st not in (200, 201):
        raise SystemExit(f"database create failed {st}: {r}")
    print(f"  database created: {DB_NAME} (id={r['id']})")
    return r["id"]


def get_dataset(db_id):
    _, r = api("GET", "/api/v1/dataset/?q=(page_size:100)")
    for d in r.get("result", []):
        if d["table_name"] == TABLE and d.get("schema") == SCHEMA:
            return d["id"]
    return None


def ensure_dataset(db_id):
    ds_id = get_dataset(db_id)
    if ds_id:
        print(f"  dataset exists: {SCHEMA}.{TABLE} (id={ds_id})")
        return ds_id
    st, r = api("POST", "/api/v1/dataset/", {
        "database": db_id, "schema": SCHEMA, "table_name": TABLE,
    })
    if st not in (200, 201):
        raise SystemExit(f"dataset create failed {st}: {r}")
    print(f"  dataset created: {SCHEMA}.{TABLE} (id={r['id']})")
    return r["id"]


def upsert_metrics(ds_id):
    _, r = api("GET", f"/api/v1/dataset/{ds_id}")
    detail = r["result"]
    by_name = {}
    for m in detail.get("metrics", []):
        by_name[m["metric_name"]] = {
            "id": m["id"], "metric_name": m["metric_name"], "expression": m["expression"],
            "metric_type": m.get("metric_type"), "verbose_name": m.get("verbose_name"),
        }
    for d in METRICS:
        merged = dict(d)
        if d["metric_name"] in by_name and by_name[d["metric_name"]].get("id"):
            merged["id"] = by_name[d["metric_name"]]["id"]
        by_name[d["metric_name"]] = merged
    st, r = api("PUT", f"/api/v1/dataset/{ds_id}", {"metrics": list(by_name.values())})
    if st != 200:
        raise SystemExit(f"metrics upsert failed {st}: {r}")
    print(f"  metrics upserted: {', '.join(m['metric_name'] for m in METRICS)}")


def main():
    login()
    db_id = ensure_database()
    ds_id = ensure_dataset(db_id)
    upsert_metrics(ds_id)
    print(f"BI provisioning complete: dataset id={ds_id} ready for charts on {SCHEMA}.{TABLE}")


if __name__ == "__main__":
    main()
