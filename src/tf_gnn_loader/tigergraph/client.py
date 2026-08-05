"""TigerGraph client for the TF_GNN loading pipeline.

Adapted from the mule harness and trimmed to what TF_GNN needs.

TigerGraph execution deadlines are passed through GSQL-TIMEOUT in
milliseconds. Client-side Requests and socket timeouts are disabled so that
large uploads and long-running queries are not terminated locally.
"""

from concurrent.futures import (
    ThreadPoolExecutor,
    TimeoutError as FuturesTimeoutError,
)
from functools import partial
import socket
from typing import cast

import requests
from pyTigerGraph import TigerGraphConnection

from tf_gnn_loader.tigergraph.settings import Settings


# pyTigerGraph 1.9.x pings *.tgcloud.io hosts inside __init__ BEFORE
# assigning restppPort/gsPort, and _req's error-recovery path reads
# both attributes. Any non-2xx ping (a paused Savanna workspace
# answers 403 on every path) therefore surfaces as
# "'TigerGraphConnection' object has no attribute 'restppPort'"
# instead of the real HTTP error. Class-level defaults keep that
# recovery path harmless; __init__ overwrites them on the instance
# immediately after the ping.
TigerGraphConnection.restppPort = "443"
TigerGraphConnection.gsPort = "443"


# TigerGraph query execution ceiling:
# 24 hours, expressed in seconds here and converted to milliseconds when sent.
_QUERY_TIMEOUT_S = 86_400.0

# Allow the local future slightly longer than TigerGraph's own execution limit.
_QUERY_TIMEOUT_GRACE_S = 300.0

# None means that Requests and the underlying socket impose no client-side
# connect/read timeout. TigerGraph's GSQL-TIMEOUT remains the server-side limit.
_HTTP_TIMEOUT: None = None


class ClientQueryTimeoutError(requests.exceptions.ReadTimeout):
    """A single installed-query call exceeded its wall-clock deadline."""


class Client:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings

        # Restore Python's normal blocking-socket behavior: no global timeout.
        socket.setdefaulttimeout(None)

        # Savanna serves REST++ and GSQL on 443; the library's 9000 /
        # 14240 defaults only apply to self-managed instances.
        port_kwargs: dict[str, str] = (
            {"restppPort": "443", "gsPort": "443"}
            if "tgcloud.io" in settings.host.lower()
            else {}
        )

        self.conn = TigerGraphConnection(
            host=settings.host,
            graphname=settings.graphname,
            gsqlSecret=settings.secret.get_secret_value(),
            **port_kwargs,
        )

        try:
            _ = self.conn.getToken(settings.secret.get_secret_value())

        except requests.exceptions.HTTPError as exc:
            response = exc.response

            if response is not None and response.status_code == 403:
                raise RuntimeError(
                    "TigerGraph rejected every request with HTTP 403 "
                    + f"(host {settings.host}). On Savanna this "
                    + "usually means the workspace is paused/stopped "
                    + "or this machine is not on its IP allowlist. "
                    + "Resume the workspace in the Savanna console "
                    + "and retry."
                ) from exc

            raise

        self._install_default_timeout()

        self._executor = ThreadPoolExecutor(
            max_workers=1,
            thread_name_prefix="tg-query",
        )

    def _install_default_timeout(self) -> None:
        """Disable client-side timeout on pyTigerGraph's private Session.

        Some pyTigerGraph 1.x methods use a private requests.Session while
        others use module-level Requests functions. This wrapper protects the
        Session-backed paths. loading.py separately controls its direct upload
        request.
        """

        session = cast(
            object,
            getattr(self.conn, "_session", None),
        )

        if not isinstance(session, requests.Session):
            return

        if getattr(
            session.request,
            "_has_default_timeout",
            False,
        ):
            return

        wrapped = partial(
            session.request,
            timeout=_HTTP_TIMEOUT,
        )

        setattr(
            wrapped,
            "_has_default_timeout",
            True,
        )

        setattr(
            session,
            "request",
            wrapped,
        )

    def run_installed_with_timeout(
        self,
        query_name: str,
        params: dict[str, object],
        timeout_s: float = _QUERY_TIMEOUT_S,
        size_limit: int = 500_000_000,
    ) -> list[object]:
        """Run an installed query with a 24-hour default ceiling.

        TigerGraph receives the limit through GSQL-TIMEOUT in milliseconds.
        The local Future waits an additional five minutes so TigerGraph, not
        Python, is normally the first component to report a timeout.
        """

        if timeout_s <= 0:
            raise ValueError("timeout_s must be greater than zero")

        timeout_ms = int(timeout_s * 1000)

        call = partial(
            self.conn.runInstalledQuery,
            query_name,
            params,
            timeout=timeout_ms,
            sizeLimit=size_limit,
        )

        future = self._executor.submit(call)

        try:
            return cast(
                list[object],
                future.result(timeout=(timeout_s + _QUERY_TIMEOUT_GRACE_S)),
            )

        except FuturesTimeoutError as exc:
            self._executor.shutdown(
                wait=False,
                cancel_futures=True,
            )

            self._executor = ThreadPoolExecutor(
                max_workers=1,
                thread_name_prefix="tg-query",
            )

            raise ClientQueryTimeoutError(
                f"installed query {query_name!r} exceeded "
                + f"{timeout_s:.0f}s deadline"
            ) from exc

    def gsql(
        self,
        statement: str,
    ) -> str:
        result = self.conn.gsql(statement)

        if not isinstance(result, str):
            raise RuntimeError(
                "expected text from gsql(), got " + type(result).__name__
            )

        return result

    @property
    def graphname(self) -> str:
        return self._settings.graphname
