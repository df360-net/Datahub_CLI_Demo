from __future__ import annotations

import requests

from .auth.base import AuthHandler


class Upstream:
    """Forwards to DataHub over a requests.Session shared with the AuthHandler.

    The handler owns login + the session's cookie jar; forward() just sends and,
    on an upstream 401, asks the handler to re-auth once and replays the request.
    """

    def __init__(
        self,
        base_url: str,
        timeout: float,
        verify_tls: bool,
        auth: AuthHandler | None = None,
    ):
        self.base_url = base_url
        self.timeout = timeout
        self.verify_tls = verify_tls
        self.auth = auth
        self.session = requests.Session()

    def attach_auth(self, auth: AuthHandler) -> None:
        self.auth = auth

    def forward(self, method: str, path_qs: str, headers: dict, body: bytes | None):
        gen = self.auth.generation() if self.auth is not None else 0
        resp = self._send(method, path_qs, headers, body)
        if resp.status_code == 401 and self.auth is not None:
            resp.close()
            if self.auth.on_upstream_unauthorized(gen):
                resp = self._send(method, path_qs, headers, body)
        return resp

    def _send(self, method: str, path_qs: str, headers: dict, body: bytes | None):
        return self.session.request(
            method=method,
            url=self.base_url + path_qs,
            headers=headers,
            data=body,
            timeout=self.timeout,
            verify=self.verify_tls,
            allow_redirects=False,
            stream=True,
        )

    def ping(self):
        return self.session.get(
            self.base_url + "/config",
            timeout=self.timeout,
            verify=self.verify_tls,
        )
