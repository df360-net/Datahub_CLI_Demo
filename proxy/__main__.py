from __future__ import annotations

from http.server import ThreadingHTTPServer

from .auth.base import LoginError
from .auth.session import SessionHandler
from .config import ProxyConfig
from .logging_setup import setup_logging
from .server import ProxyHandler
from .upstream import Upstream


def main() -> None:
    cfg = ProxyConfig.from_env()
    cfg.enforce_loopback()
    logger = setup_logging(cfg.log_level, cfg.log_format)

    if not cfg.datahub_user or not cfg.datahub_pw:
        raise SystemExit("DATAHUB_USER and DATAHUB_PW are required for session auth")

    upstream = Upstream(cfg.datahub_url, cfg.upstream_timeout_seconds, cfg.upstream_verify_tls)
    auth = SessionHandler(
        session=upstream.session,
        datahub_url=cfg.datahub_url,
        user=cfg.datahub_user,
        pw=cfg.datahub_pw,
        refresh_minutes=cfg.session_refresh_minutes,
        timeout=cfg.upstream_timeout_seconds,
        verify_tls=cfg.upstream_verify_tls,
        logger=logger,
    )

    try:
        auth.login()
    except LoginError as exc:
        raise SystemExit(f"eager login failed, refusing to start: {exc}")

    upstream.attach_auth(auth)
    auth.start()

    ProxyHandler.cfg = cfg
    ProxyHandler.upstream = upstream
    ProxyHandler.logger = logger

    server = ThreadingHTTPServer((cfg.proxy_bind, cfg.proxy_port), ProxyHandler)
    logger.info(
        "Auth Proxy v0.2 (session auth) on http://%s:%d -> %s",
        cfg.proxy_bind,
        cfg.proxy_port,
        cfg.datahub_url,
    )
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        logger.info("shutting down")
    finally:
        auth.stop()
        server.shutdown()
        server.server_close()


if __name__ == "__main__":
    main()
