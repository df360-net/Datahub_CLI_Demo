import requests


def test_healthz_ok(proxy_server):
    r = requests.get(f"{proxy_server}/proxy/healthz")
    assert r.status_code == 200
    assert r.json()["upstream"] == "reachable"


def test_unknown_proxy_endpoint_404(proxy_server):
    assert requests.get(f"{proxy_server}/proxy/nope").status_code == 404


def test_passthrough_injects_cookies_and_strips_incoming(proxy_server, mock_upstream):
    r = requests.post(
        f"{proxy_server}/openapi/v3/x",
        data=b"hello-body",
        headers={"Authorization": "Bearer leaked", "Cookie": "stale=1"},
    )
    assert r.status_code == 200
    fwd = [x for x in mock_upstream.received if x["path"] == "/openapi/v3/x"][-1]
    cookie = fwd["headers"].get("cookie", "")
    assert "PLAY_SESSION=sess-abc" in cookie
    assert "actor=" in cookie
    assert "stale=1" not in cookie            # incoming Cookie stripped
    assert "authorization" not in fwd["headers"]  # incoming Authorization stripped
    assert fwd["body"] == b"hello-body"       # body forwarded verbatim


def test_status_preserved_verbatim(proxy_server, mock_upstream):
    mock_upstream.responder = lambda m, p, h, b: (
        (404, {}, b"missing") if p == "/x" else mock_upstream.default_responder(m, p, h, b)
    )
    assert requests.get(f"{proxy_server}/x").status_code == 404


def test_upstream_set_cookie_stripped(proxy_server, mock_upstream):
    mock_upstream.responder = lambda m, p, h, b: (
        (200, {"Set-Cookie": "leak=1; Path=/"}, b"ok") if p == "/y"
        else mock_upstream.default_responder(m, p, h, b)
    )
    r = requests.get(f"{proxy_server}/y")
    assert "set-cookie" not in {k.lower() for k in r.headers}


def test_401_triggers_relogin_and_replay(proxy_server, mock_upstream):
    state = {"hits": 0}

    def responder(m, p, h, b):
        if p == "/logIn":
            return 200, {"Set-Cookie": ["PLAY_SESSION=s2; Path=/", "actor=a; Path=/"]}, b""
        if p == "/z":
            state["hits"] += 1
            return (401, {}, b"expired") if state["hits"] == 1 else (200, {}, b"ok")
        return 200, {}, b"ok"

    mock_upstream.responder = responder
    r = requests.get(f"{proxy_server}/z")
    assert r.status_code == 200
    assert state["hits"] == 2  # original 401 + one replay


def test_double_401_does_not_loop(proxy_server, mock_upstream):
    def responder(m, p, h, b):
        if p == "/logIn":
            return 200, {"Set-Cookie": ["PLAY_SESSION=s3; Path=/", "actor=a; Path=/"]}, b""
        if p == "/dead":
            return 401, {}, b"still expired"
        return 200, {}, b"ok"

    mock_upstream.responder = responder
    assert requests.get(f"{proxy_server}/dead").status_code == 401
