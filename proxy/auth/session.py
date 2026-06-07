from __future__ import annotations

import logging
import threading
import time

import requests

from .base import AuthHandler, LoginError


class SessionHandler(AuthHandler):
    """Session (SSO-style) auth against the DataHub frontend's POST /logIn.

    Login captures the whole cookie jar (PLAY_SESSION + actor + …) onto the
    shared session, so every forwarded request carries it automatically. A
    background timer re-logs in periodically; an upstream 401 triggers a single
    re-login + replay.
    """

    def __init__(
        self,
        session: requests.Session,
        datahub_url: str,
        user: str,
        pw: str,
        refresh_minutes: float,
        timeout: float,
        verify_tls: bool,
        logger: logging.Logger,
    ):
        self._session = session
        self._url = datahub_url
        self._user = user
        self._pw = pw
        self._refresh_seconds = refresh_minutes * 60.0
        self._timeout = timeout
        self._verify = verify_tls
        self._logger = logger

        self._lock = threading.Lock()
        self._generation = 0
        self._last_login_monotonic: float | None = None
        self._last_login_status: int | None = None
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None

    def login(self) -> None:
        with self._lock:
            self._login_locked()

    def _login_locked(self) -> None:
        # Drop any stale jar so a failed re-login never leaves a half-valid state.
        self._session.cookies.clear()
        try:
            resp = self._session.post(
                f"{self._url}/logIn",
                json={"username": self._user, "password": self._pw},
                timeout=self._timeout,
                verify=self._verify,
                allow_redirects=False,
            )
        except requests.RequestException as exc:
            raise LoginError(f"POST /logIn failed: {type(exc).__name__}") from exc

        self._last_login_status = resp.status_code
        if resp.status_code != 200:
            raise LoginError(f"POST /logIn returned {resp.status_code} ({resp.reason})")
        if "PLAY_SESSION" not in self._session.cookies:
            raise LoginError("POST /logIn returned 200 but set no PLAY_SESSION cookie")

        self._generation += 1
        self._last_login_monotonic = time.monotonic()
        self._logger.info(
            "session login ok (status 200, gen %d, cookies: %s)",
            self._generation,
            ",".join(sorted(self._session.cookies.keys())),
        )

    def generation(self) -> int:
        return self._generation

    def on_upstream_unauthorized(self, seen_generation: int) -> bool:
        with self._lock:
            if self._generation > seen_generation:
                # Another thread already re-logged in since this request was sent.
                return True
            try:
                self._login_locked()
                return True
            except LoginError as exc:
                self._logger.warning("re-login after upstream 401 failed: %s", exc)
                return False

    def start(self) -> None:
        self._thread = threading.Thread(
            target=self._refresh_loop, name="session-refresh", daemon=True
        )
        self._thread.start()

    def _refresh_loop(self) -> None:
        while not self._stop_event.wait(self._refresh_seconds):
            try:
                self.login()
            except LoginError as exc:
                self._logger.warning("background session refresh failed: %s", exc)

    def stop(self) -> None:
        self._stop_event.set()

    def health(self) -> dict:
        age = None
        if self._last_login_monotonic is not None:
            age = round(time.monotonic() - self._last_login_monotonic, 1)
        return {
            "session_age_seconds": age,
            "session_refresh_minutes": self._refresh_seconds / 60.0,
            "last_login_status": self._last_login_status,
        }
