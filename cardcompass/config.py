from __future__ import annotations

import os
import urllib.parse

from dotenv import load_dotenv
from pydantic import BaseModel


class AppConfig(BaseModel):
    mssql_host: str
    mssql_port: int = 1433
    mssql_database: str = "DCF_DB"
    mssql_user: str
    mssql_password: str
    mssql_driver: str = "ODBC Driver 18 for SQL Server"
    inbound_dir: str = "./data/inbound"
    proxy_url: str = "http://127.0.0.1:8080"
    source_app_name: str = "CardCompass"
    enterprise_pid: str = "CARDC"
    log_level: str = "INFO"

    @classmethod
    def from_env(cls) -> "AppConfig":
        load_dotenv()

        def req(key: str) -> str:
            val = os.getenv(key)
            if not val:
                raise SystemExit(f"{key} is required (set it in .env)")
            return val

        return cls(
            mssql_host=req("MSSQL_HOST"),
            mssql_port=int(os.getenv("MSSQL_PORT", "1433")),
            mssql_database=os.getenv("MSSQL_DATABASE", "DCF_DB"),
            mssql_user=req("MSSQL_USER"),
            mssql_password=req("MSSQL_PASSWORD"),
            mssql_driver=os.getenv("MSSQL_DRIVER", "ODBC Driver 18 for SQL Server"),
            inbound_dir=os.getenv("INBOUND_DIR", "./data/inbound"),
            proxy_url=os.getenv("PROXY_URL", "http://127.0.0.1:8080").rstrip("/"),
            source_app_name=os.getenv("SOURCE_APP_NAME", "CardCompass"),
            enterprise_pid=req("ENTERPRISE_PID"),
            log_level=os.getenv("LOG_LEVEL", "INFO").upper(),
        )

    @property
    def odbc_connection_string(self) -> str:
        # Encrypt=yes is Driver 18's default; TrustServerCertificate=yes is
        # required for Murphy's self-signed cert (same reason sqlcmd needs -C).
        return (
            f"DRIVER={{{self.mssql_driver}}};"
            f"SERVER={self.mssql_host},{self.mssql_port};"
            f"DATABASE={self.mssql_database};"
            f"UID={self.mssql_user};PWD={self.mssql_password};"
            "Encrypt=yes;TrustServerCertificate=yes"
        )

    @property
    def sqlalchemy_url(self) -> str:
        # odbc_connect form sidesteps password/special-char escaping in the URL.
        return "mssql+pyodbc:///?odbc_connect=" + urllib.parse.quote_plus(
            self.odbc_connection_string
        )
