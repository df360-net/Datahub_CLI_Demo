from __future__ import annotations

import os

from dotenv import load_dotenv
from pydantic import BaseModel

_LOOPBACK = {"127.0.0.1", "::1", "localhost"}


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


class ProxyConfig(BaseModel):
    proxy_port: int = 8080
    proxy_bind: str = "127.0.0.1"
    proxy_allow_non_loopback: bool = False
    datahub_url: str
    datahub_user: str | None = None
    datahub_pw: str | None = None
    session_refresh_minutes: float = 20.0
    upstream_timeout_seconds: float = 60.0
    upstream_verify_tls: bool = True
    log_level: str = "INFO"
    log_format: str = "text"

    @classmethod
    def from_env(cls) -> "ProxyConfig":
        load_dotenv()
        url = os.getenv("DATAHUB_URL")
        if not url:
            raise SystemExit("DATAHUB_URL is required (set it in .env)")
        return cls(
            proxy_port=int(os.getenv("PROXY_PORT", "8080")),
            proxy_bind=os.getenv("PROXY_BIND", "127.0.0.1"),
            proxy_allow_non_loopback=_env_bool("PROXY_ALLOW_NON_LOOPBACK", False),
            datahub_url=url.rstrip("/"),
            datahub_user=os.getenv("DATAHUB_USER"),
            datahub_pw=os.getenv("DATAHUB_PW"),
            session_refresh_minutes=float(os.getenv("SESSION_REFRESH_MINUTES", "20")),
            upstream_timeout_seconds=float(os.getenv("UPSTREAM_TIMEOUT_SECONDS", "60")),
            upstream_verify_tls=_env_bool("UPSTREAM_VERIFY_TLS", True),
            log_level=os.getenv("LOG_LEVEL", "INFO").upper(),
            log_format=os.getenv("LOG_FORMAT", "text").lower(),
        )

    @property
    def bind_is_loopback(self) -> bool:
        return self.proxy_bind in _LOOPBACK

    def enforce_loopback(self) -> None:
        if not self.bind_is_loopback and not self.proxy_allow_non_loopback:
            raise SystemExit(
                f"PROXY_BIND={self.proxy_bind!r} is not loopback. Refusing to start. "
                "Set PROXY_ALLOW_NON_LOOPBACK=1 to override (discouraged)."
            )
