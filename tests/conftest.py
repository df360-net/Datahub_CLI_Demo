from __future__ import annotations

import importlib.util
import logging
import os
import pathlib
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

# Deterministic PID for URN tests; force it (not setdefault) so a developer's
# shell/.env ENTERPRISE_PID can't make the hermetic URN assertions fail.
os.environ["ENTERPRISE_PID"] = "TEST"

_REPO = pathlib.Path(__file__).resolve().parent.parent
_LOG = logging.getLogger("test")
_LOG.addHandler(logging.NullHandler())


class MockUpstream:
    """A stand-in DataHub frontend. Records requests; responder is swappable
    per-test. Default: /logIn -> 200 + PLAY_SESSION + actor cookies."""

    def __init__(self):
        self.received: list[dict] = []
        self.responder = self.default_responder
        self._server = ThreadingHTTPServer(("127.0.0.1", 0), self._make_handler())
        self.port = self._server.server_address[1]
        self.url = f"http://127.0.0.1:{self.port}"
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)

    def start(self):
        self._thread.start()

    def stop(self):
        self._server.shutdown()
        self._server.server_close()

    @staticmethod
    def default_responder(method, path, headers, body):
        if path == "/logIn":
            return 200, {"Set-Cookie": [
                "PLAY_SESSION=sess-abc; Path=/",
                "actor=urn:li:corpuser:tester; Path=/",
            ]}, b""
        if path.startswith("/config"):
            return 200, {"Content-Type": "application/json"}, b'{"ok":true}'
        return 200, {}, b"ok"

    def _make_handler(self):
        mock = self

        class _H(BaseHTTPRequestHandler):
            def log_message(self, *a):
                pass

            def _do(self):
                length = int(self.headers.get("Content-Length", 0))
                body = self.rfile.read(length) if length else b""
                mock.received.append({
                    "method": self.command,
                    "path": self.path,
                    "headers": {k.lower(): v for k, v in self.headers.items()},
                    "body": body,
                })
                status, hdrs, resp = mock.responder(self.command, self.path, self.headers, body)
                self.send_response_only(status)
                for k, v in hdrs.items():
                    for item in (v if isinstance(v, list) else [v]):
                        self.send_header(k, item)
                self.send_header("Content-Length", str(len(resp)))
                self.end_headers()
                self.wfile.write(resp)

            do_GET = do_POST = do_PUT = do_DELETE = do_PATCH = _do

        return _H


@pytest.fixture
def mock_upstream():
    m = MockUpstream()
    m.start()
    yield m
    m.stop()


@pytest.fixture
def proxy_server(mock_upstream):
    """A real proxy ThreadingHTTPServer forwarding to the mock upstream,
    after an eager login. Yields the proxy base URL."""
    from proxy.auth.session import SessionHandler
    from proxy.config import ProxyConfig
    from proxy.server import ProxyHandler
    from proxy.upstream import Upstream

    cfg = ProxyConfig(datahub_url=mock_upstream.url, datahub_user="u", datahub_pw="p")
    up = Upstream(cfg.datahub_url, cfg.upstream_timeout_seconds, cfg.upstream_verify_tls)
    auth = SessionHandler(up.session, cfg.datahub_url, cfg.datahub_user, cfg.datahub_pw,
                          cfg.session_refresh_minutes, cfg.upstream_timeout_seconds,
                          cfg.upstream_verify_tls, _LOG)
    auth.login()
    up.attach_auth(auth)

    # Per-fixture handler subclass so state isn't shared on the global class
    # across tests (order-independent; safe under pytest-xdist).
    bound = type("BoundProxyHandler", (ProxyHandler,),
                 {"cfg": cfg, "upstream": up, "logger": _LOG})
    server = ThreadingHTTPServer(("127.0.0.1", 0), bound)
    port = server.server_address[1]
    threading.Thread(target=server.serve_forever, daemon=True).start()
    yield f"http://127.0.0.1:{port}"
    server.shutdown()
    server.server_close()
    auth.stop()


@pytest.fixture(scope="session")
def genmod():
    """Load tools/generate_daily_file.py as a module (it isn't a package)."""
    path = _REPO / "tools" / "generate_daily_file.py"
    spec = importlib.util.spec_from_file_location("generate_daily_file", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod
