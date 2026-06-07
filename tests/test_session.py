import logging
import threading

import pytest
import requests

from proxy.auth.base import LoginError
from proxy.auth.session import SessionHandler

_LOG = logging.getLogger("test")


def _handler(mock):
    return SessionHandler(requests.Session(), mock.url, "u", "p", 20, 10, False, _LOG)


def test_eager_login_ok(mock_upstream):
    h = _handler(mock_upstream)
    h.login()
    assert h.generation() == 1
    assert h.health()["last_login_status"] == 200


def test_login_non_200_refused(mock_upstream):
    mock_upstream.responder = lambda m, p, h, b: (401, {}, b"bad creds")
    with pytest.raises(LoginError):
        _handler(mock_upstream).login()


def test_login_200_but_no_cookie_refused(mock_upstream):
    mock_upstream.responder = lambda m, p, h, b: (200, {}, b"")
    with pytest.raises(LoginError):
        _handler(mock_upstream).login()


def test_relogin_bumps_generation(mock_upstream):
    h = _handler(mock_upstream)
    h.login()
    assert h.on_upstream_unauthorized(seen_generation=1) is True
    assert h.generation() == 2


def test_concurrent_401_storm_single_relogin(mock_upstream):
    logins = {"n": 0}

    def responder(m, p, h, b):
        if p == "/logIn":
            logins["n"] += 1
            return 200, {"Set-Cookie": ["PLAY_SESSION=s; Path=/", "actor=a; Path=/"]}, b""
        return 200, {}, b"ok"

    mock_upstream.responder = responder
    h = _handler(mock_upstream)
    h.login()  # logins -> 1
    gen0 = h.generation()

    results = []

    def worker():
        results.append(h.on_upstream_unauthorized(gen0))

    threads = [threading.Thread(target=worker) for _ in range(20)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert all(results)
    assert h.generation() == gen0 + 1   # exactly one re-login took effect
    assert logins["n"] == 2             # initial + a single re-login (lock collapsed the storm)
