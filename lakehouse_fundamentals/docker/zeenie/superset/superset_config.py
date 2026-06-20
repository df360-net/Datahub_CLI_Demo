"""Superset config for the Zeenie lab — lean single-container, SQLite metadata.

Loaded automatically (mounted onto PYTHONPATH at /app/pythonpath). See docs/Sales_BI_Design.md.
"""
import os

# Stable across restarts or sessions break. Lab-only value; override via env in real use.
SECRET_KEY = os.environ.get("SUPERSET_SECRET_KEY", "zeenie-lab-superset-not-a-real-secret")

# Metadata DB = SQLite on the persisted superset-home volume (single-user lab instance).
SQLALCHEMY_DATABASE_URI = "sqlite:////app/superset_home/superset.db"

ROW_LIMIT = 50000
SUPERSET_WEBSERVER_TIMEOUT = 120
SQLLAB_TIMEOUT = 120

# Right-click drill-down on the dashboard: DRILL_TO_DETAIL = raw underlying rows behind an
# aggregate (e.g. click Electronics -> the order lines from vw_sales_report); DRILL_BY = re-pivot a
# metric by another dimension on the fly. Cross-filtering (click one chart -> filters the others) is
# a per-dashboard toggle in the UI.
FEATURE_FLAGS = {
    "DRILL_TO_DETAIL": True,
    "DRILL_BY": True,
}
