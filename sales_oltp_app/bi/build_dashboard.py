"""Build the "Sales Overview" dashboard as code — charts + layout via the Superset REST API.

Idempotent: charts are get-or-created by name (params + query_context updated in place); the dashboard
is get-or-created by slug with a generated grid layout (position_json), then every chart is linked to
it. Each chart persists BOTH params (form_data) and query_context, so it renders interactively AND
headless (thumbnails/alerts/export). Reuses auth/api from setup_superset.py (run that first).
See docs/Sales_BI_Design.md.  Run:  sales_oltp_app/.venv/Scripts/python.exe bi/build_dashboard.py
"""
import importlib.util
import json
import os

HERE = os.path.dirname(os.path.abspath(__file__))
_spec = importlib.util.spec_from_file_location("setup_superset", os.path.join(HERE, "setup_superset.py"))
ss = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(ss)

DS = "1__table"
DS_ID = 1
DASH_TITLE = "Sales Overview"
DASH_SLUG = "sales-overview"


def form_data(**kw):
    base = {"datasource": DS, "adhoc_filters": []}
    base.update(kw)
    return base


def query(metrics, columns=None, orderby=None, row_limit=0, time_grain=None):
    q = {"metrics": metrics, "columns": columns or [], "filters": [],
         "extras": {"having": "", "where": ""}, "orderby": orderby or [],
         "row_limit": row_limit, "annotation_layers": [], "series_limit": 0, "order_desc": True}
    if time_grain:
        q["extras"]["time_grain_sqla"] = time_grain
        q["is_timeseries"] = True
    return q


# temporal x-axis column (echarts timeseries needs the grain-bearing axis in query_context.columns)
def axis(col, grain):
    return {"timeGrain": grain, "columnType": "BASE_AXIS", "sqlExpression": col,
            "label": col, "expressionType": "SQL"}


# key -> (slice_name, viz_type, form_data, query, layout width)
CHARTS = {
    "bn_rev": ("Total Revenue", "big_number_total",
               form_data(viz_type="big_number_total", metric="revenue", y_axis_format="$,.2s"),
               query(["revenue"]), 4),
    "bn_ord": ("Orders", "big_number_total",
               form_data(viz_type="big_number_total", metric="orders", y_axis_format=",.0f"),
               query(["orders"]), 4),
    "bn_units": ("Units Sold", "big_number_total",
                 form_data(viz_type="big_number_total", metric="units", y_axis_format=",.0f"),
                 query(["units"]), 4),
    "ts_month": ("Revenue by Month", "echarts_timeseries_bar",
                 form_data(viz_type="echarts_timeseries_bar", x_axis="order_date", time_grain_sqla="P1M",
                           granularity_sqla="order_date", metrics=["revenue"], groupby=[],
                           row_limit=10000, x_axis_sort_asc=True),
                 {**query(["revenue"], columns=[axis("order_date", "P1M")], row_limit=10000, time_grain="P1M"),
                  "series_columns": [], "granularity": "order_date"}, 8),
    "pie_seg": ("Revenue by Segment", "pie",
                form_data(viz_type="pie", groupby=["segment"], metric="revenue", row_limit=100),
                query(["revenue"], columns=["segment"], orderby=[["revenue", False]], row_limit=100), 4),
    "tbl_cat": ("Top Categories", "table",
                form_data(viz_type="table", query_mode="aggregate", groupby=["category_name"],
                          metrics=["revenue", "units", "orders"], order_desc=True, row_limit=10),
                query(["revenue", "units", "orders"], columns=["category_name"],
                      orderby=[["revenue", False]], row_limit=10), 6),
    "tbl_reg": ("Revenue by Region & Channel", "table",
                form_data(viz_type="table", query_mode="aggregate", groupby=["region", "channel"],
                          metrics=["revenue", "orders"], order_desc=True, row_limit=50),
                query(["revenue", "orders"], columns=["region", "channel"],
                      orderby=[["revenue", False]], row_limit=50), 6),
}

ROWS = [["bn_rev", "bn_ord", "bn_units"], ["ts_month", "pie_seg"], ["tbl_cat", "tbl_reg"]]


def chart_body(name, viz, fd, q):
    qc = {"datasource": {"id": DS_ID, "type": "table"}, "force": False,
          "queries": [q], "form_data": fd, "result_format": "json", "result_type": "full"}
    return {"slice_name": name, "viz_type": viz, "datasource_id": DS_ID, "datasource_type": "table",
            "params": json.dumps(fd), "query_context": json.dumps(qc)}


def list_charts():
    _, r = ss.api("GET", "/api/v1/chart/?q=(page_size:100)")
    return {c["slice_name"]: c["id"] for c in r.get("result", [])}


def upsert_chart(name, viz, fd, q, existing):
    body = chart_body(name, viz, fd, q)
    if name in existing:
        cid = existing[name]
        st, r = ss.api("PUT", f"/api/v1/chart/{cid}", body)
        if st != 200:
            raise SystemExit(f"chart update failed {name} {st}: {r}")
        return cid
    st, r = ss.api("POST", "/api/v1/chart/", body)
    if st not in (200, 201):
        raise SystemExit(f"chart create failed {name} {st}: {r}")
    return r["id"]


def build_position(ids):
    pos = {
        "DASHBOARD_VERSION_KEY": "v2",
        "ROOT_ID": {"type": "ROOT", "id": "ROOT_ID", "children": ["GRID_ID"]},
        "GRID_ID": {"type": "GRID", "id": "GRID_ID", "parents": ["ROOT_ID"], "children": []},
        "HEADER_ID": {"type": "HEADER", "id": "HEADER_ID", "meta": {"text": DASH_TITLE}},
    }
    for ri, row in enumerate(ROWS, 1):
        row_id = f"ROW-{ri}"
        pos["GRID_ID"]["children"].append(row_id)
        pos[row_id] = {"type": "ROW", "id": row_id, "parents": ["ROOT_ID", "GRID_ID"],
                       "children": [], "meta": {"background": "BACKGROUND_TRANSPARENT"}}
        for key in row:
            ckey = f"CHART-{key}"
            pos[row_id]["children"].append(ckey)
            pos[ckey] = {"type": "CHART", "id": ckey,
                         "parents": ["ROOT_ID", "GRID_ID", row_id], "children": [],
                         "meta": {"chartId": ids[key], "width": CHARTS[key][4], "height": 50,
                                  "sliceName": CHARTS[key][0]}}
    return pos


def get_dashboard():
    _, r = ss.api("GET", "/api/v1/dashboard/?q=(page_size:100)")
    for d in r.get("result", []):
        if d.get("slug") == DASH_SLUG:
            return d["id"]
    return None


def main():
    ss.login()
    existing = list_charts()
    ids = {}
    for key, (name, viz, fd, q, _w) in CHARTS.items():
        ids[key] = upsert_chart(name, viz, fd, q, existing)
        print(f"  chart {name!r:32} id={ids[key]}")

    body = {"dashboard_title": DASH_TITLE, "slug": DASH_SLUG,
            "position_json": json.dumps(build_position(ids)), "published": True}
    dash_id = get_dashboard()
    if dash_id:
        st, r = ss.api("PUT", f"/api/v1/dashboard/{dash_id}", body)
        action = "updated"
    else:
        st, r = ss.api("POST", "/api/v1/dashboard/", body)
        action = "created"
        dash_id = r.get("id")
    if st not in (200, 201):
        raise SystemExit(f"dashboard {action} failed {st}: {r}")

    for key, cid in ids.items():
        st2, r2 = ss.api("PUT", f"/api/v1/chart/{cid}", {"dashboards": [dash_id]})
        if st2 != 200:
            raise SystemExit(f"chart->dashboard link failed {key} {st2}: {r2}")
    print(f"dashboard {action}: {DASH_TITLE!r} (id={dash_id}), {len(ids)} charts linked -> "
          f"{ss.SS_URL}/superset/dashboard/{DASH_SLUG}/")


if __name__ == "__main__":
    main()
