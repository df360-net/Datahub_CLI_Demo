import pytest

from proxy.config import ProxyConfig


def test_loopback_default_ok():
    ProxyConfig(datahub_url="http://x", proxy_bind="127.0.0.1").enforce_loopback()


def test_non_loopback_refused():
    cfg = ProxyConfig(datahub_url="http://x", proxy_bind="0.0.0.0")
    with pytest.raises(SystemExit):
        cfg.enforce_loopback()


def test_non_loopback_override_allowed():
    cfg = ProxyConfig(datahub_url="http://x", proxy_bind="0.0.0.0", proxy_allow_non_loopback=True)
    cfg.enforce_loopback()
