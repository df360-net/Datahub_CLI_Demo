"""Settings - python-dotenv + pydantic BaseModel (workplace-aligned stack)."""
import os

from dotenv import load_dotenv
from pydantic import BaseModel

HERE = os.path.dirname(os.path.abspath(__file__))


class Settings(BaseModel):
    gms_server: str
    token: str
    platform_instance: str
    env: str = "PROD"

    @classmethod
    def load(cls) -> "Settings":
        # repo-root .env first (where DATAHUB_ACCESS_TOKEN lives), then a local sales360/.env
        for rel in ("../.env", ".env"):
            path = os.path.normpath(os.path.join(HERE, rel))
            if os.path.exists(path):
                load_dotenv(path, override=False)
        token = os.environ.get("DATAHUB_ACCESS_TOKEN")
        if not token:
            raise SystemExit("DATAHUB_ACCESS_TOKEN not set in .env")
        return cls(
            gms_server=os.environ.get("SALES360_GMS_SERVER", "http://192.168.0.16:8080"),
            token=token,
            platform_instance=os.environ.get("SALES360_PID", "SAL360"),
            env=os.environ.get("SALES360_ENV", "PROD"),
        )
