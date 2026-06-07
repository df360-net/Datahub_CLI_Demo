from __future__ import annotations

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from .config import AppConfig


def make_engine(cfg: AppConfig) -> Engine:
    """SQLAlchemy 2.0 engine over pyodbc + ODBC Driver 18.

    fast_executemany speeds the stage layer's bulk row inserts; pool_pre_ping
    drops dead connections (the ETL runs daily against a LAN host).
    """
    return create_engine(
        cfg.sqlalchemy_url,
        fast_executemany=True,
        pool_pre_ping=True,
    )
