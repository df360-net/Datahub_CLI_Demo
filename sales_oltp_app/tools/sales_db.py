"""Config-driven SQLAlchemy engine factory for the Sales OLTP database.

Connection settings resolve from the process environment first, then from the
sales_oltp_app/.env file. This is the 12-factor split that lets the SAME code run
on Lenovo in dev (MYSQL_HOST=192.168.0.21, from .env) and inside the airflow
container in prod (MYSQL_HOST=host.docker.internal, from env vars) with no change.
"""
import os
from pathlib import Path

from dotenv import dotenv_values
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

_ENV = dotenv_values(Path(__file__).resolve().parents[1] / ".env")


def cfg(key: str, default: str | None = None) -> str | None:
    return os.environ.get(key) or _ENV.get(key) or default


def get_engine(role: str = "app", echo: bool = False) -> Engine:
    """role='app' -> DML user (sales_app); role='extract' -> read-only (sales_extract)."""
    if role == "app":
        user, pwd = cfg("SALES_APP_USER"), cfg("SALES_APP_PWD")
    elif role == "extract":
        user, pwd = cfg("SALES_EXTRACT_USER"), cfg("SALES_EXTRACT_PWD")
    else:
        raise ValueError(f"unknown role: {role!r}")

    host = cfg("MYSQL_HOST", "127.0.0.1")
    port = cfg("MYSQL_PORT", "3306")
    db = cfg("MYSQL_DB", "sales_oltp")
    url = f"mysql+pymysql://{user}:{pwd}@{host}:{port}/{db}"
    return create_engine(url, echo=echo, pool_pre_ping=True, future=True)
