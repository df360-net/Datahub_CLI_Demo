from __future__ import annotations

from abc import ABC, abstractmethod


class LoginError(RuntimeError):
    """Raised when an auth handler cannot establish a valid credential."""


class AuthHandler(ABC):
    """Manages credentials on the shared requests.Session.

    v1 ships exactly one implementation (SessionHandler). The ABC exists so a
    future mode (OIDC client-credentials, mTLS, Bearer) is a mechanical add:
    each mode configures the shared session — cookies for session auth, an
    Authorization header for token auth — rather than touching forward logic.
    """

    @abstractmethod
    def login(self) -> None:
        """Authenticate, populating the shared session. Raise LoginError on failure."""

    @abstractmethod
    def generation(self) -> int:
        """Monotonic counter, bumped on each successful login. Used to collapse
        concurrent 401 recoveries into a single re-login."""

    @abstractmethod
    def on_upstream_unauthorized(self, seen_generation: int) -> bool:
        """Called on an upstream 401. `seen_generation` is the generation in
        effect when the failing request was sent. Return True if the caller
        should replay the request, False to surface the 401 as-is."""

    @abstractmethod
    def health(self) -> dict:
        """Session age / last-login status for /proxy/healthz."""

    def start(self) -> None:
        """Begin background work (e.g. refresh timer). Default: no-op."""

    def stop(self) -> None:
        """Stop background work. Default: no-op."""
