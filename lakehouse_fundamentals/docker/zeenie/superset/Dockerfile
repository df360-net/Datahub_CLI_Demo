# Lean Superset image = official base + the Dremio Arrow Flight SQL driver (not in the base image).
# Pinned to the version pulled into the Zeenie lab. See docs/Sales_BI_Design.md.
FROM apache/superset:6.1.0
USER root
# Install into Superset's uv-managed venv (NOT the system pip on PATH), and --no-deps so it does
# NOT pull SQLAlchemy up to 2.0: Superset 6.1 pins SQLAlchemy 1.4.x, and the Flight driver runs
# fine on 1.4 (verified). The venv already provides pyarrow.
RUN uv pip install --python /app/.venv/bin/python --no-deps --link-mode=copy sqlalchemy-dremio==3.0.5
USER superset
