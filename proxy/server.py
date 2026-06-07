from __future__ import annotations

import json
import time
from http.server import BaseHTTPRequestHandler

from .config import ProxyConfig
from .health import build_health
from .upstream import Upstream

# Stripped from the incoming request before forwarding.
# Authorization + Cookie: the proxy is the sole authority on auth (v0.2 injects it).
# Host/Content-Length: requests sets these from the URL and body.
# The rest are hop-by-hop headers (RFC 7230 6.1) that must not be forwarded.
_STRIP_REQUEST = {
    "host",
    "authorization",
    "cookie",
    "content-length",
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailers",
    "transfer-encoding",
    "upgrade",
}

# Stripped from the upstream response before relaying.
# Set-Cookie: cookies are proxy-internal state, never propagated to the app.
# Content-Encoding/Content-Length/Transfer-Encoding: requests decodes the body,
# so we recompute Content-Length and drop the encoding markers.
_STRIP_RESPONSE = {
    "set-cookie",
    "content-encoding",
    "content-length",
    "transfer-encoding",
    "connection",
    "keep-alive",
}


class ProxyHandler(BaseHTTPRequestHandler):
    cfg: ProxyConfig
    upstream: Upstream
    logger = None

    protocol_version = "HTTP/1.1"
    server_version = "datahub-auth-proxy/0.1"

    def do_GET(self):
        self._dispatch()

    def do_POST(self):
        self._dispatch()

    def do_PUT(self):
        self._dispatch()

    def do_DELETE(self):
        self._dispatch()

    def do_PATCH(self):
        self._dispatch()

    def _dispatch(self):
        try:
            if self.path.startswith("/proxy/"):
                self._handle_local()
            else:
                self._forward()
        except Exception as exc:
            self.logger.exception("proxy failure on %s %s", self.command, self.path)
            self._send_json(502, {"error": "proxy_failure", "detail": type(exc).__name__})

    def _handle_local(self):
        if self.path.split("?", 1)[0] == "/proxy/healthz":
            code, payload = build_health(self.cfg, self.upstream)
            self._send_json(code, payload)
        else:
            self._send_json(404, {"error": "unknown_proxy_endpoint", "path": self.path})

    def _forward(self):
        body = self._read_body()
        out_headers = {
            k: v for k, v in self.headers.items() if k.lower() not in _STRIP_REQUEST
        }
        t0 = time.monotonic()
        resp = self.upstream.forward(self.command, self.path, out_headers, body)
        content = resp.content
        latency_ms = (time.monotonic() - t0) * 1000.0
        self.logger.info(
            "%s %s -> %d (%.0f ms, %d bytes)",
            self.command,
            self.path,
            resp.status_code,
            latency_ms,
            len(content),
        )
        self._relay(resp.status_code, resp.headers.items(), content)

    def _read_body(self) -> bytes | None:
        length = int(self.headers.get("Content-Length", 0))
        return self.rfile.read(length) if length else None

    def _relay(self, status: int, headers, content: bytes):
        self.send_response_only(status)
        for k, v in headers:
            if k.lower() not in _STRIP_RESPONSE:
                self.send_header(k, v)
        self.send_header("Content-Length", str(len(content)))
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(content)

    def _send_json(self, code: int, payload: dict):
        data = json.dumps(payload).encode("utf-8")
        self.send_response_only(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def log_message(self, fmt, *args):
        if self.logger is not None:
            self.logger.debug("http: " + fmt, *args)
