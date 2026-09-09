"""HTTP transport for the MLB producers.

statsapi.mlb.com **refuses this droplet's IP outright** -- measured 2026-09-09,
HTTP 406 to every header variant tried (curl_cffi browser impersonation, plain
urllib, browser UA, ``curl/8.0``, no headers at all). It is not a rate limit and
not a User-Agent problem: it is destination-side IP filtering, the same class as
stats.nba.com hanging on datacenter IPs.

A ProxyBonanza endpoint clears it on the first try. Decodo does **not** -- 0/5
ports, ``CONNECT tunnel failed`` -- so the NCAA transport config does not
transfer here, and defaulting to the NCAA vendor would fail confusingly.

Baseball Savant needs no proxy at all (200 direct), so it is fetched directly
unless a proxy is explicitly requested.

Rate/pace is env-only -- never edit it into a caller:
  SDV_MLB_WORKERS     concurrent fetches                    (default 8)
  SDV_MLB_TIMEOUT     per-request seconds                   (default 45)
  SDV_MLB_RETRIES     attempts per URL before giving up     (default 3)
  SDV_MLB_SLEEP       seconds between sequential requests   (default 0)
"""

from __future__ import annotations

import os
import pathlib
import random
import threading
import time

STATSAPI = "https://statsapi.mlb.com/api"
SAVANT = "https://baseballsavant.mlb.com"

TIMEOUT = int(os.environ.get("SDV_MLB_TIMEOUT", "45"))
RETRIES = int(os.environ.get("SDV_MLB_RETRIES", "3"))
WORKERS = int(os.environ.get("SDV_MLB_WORKERS", "8"))
SLEEP = float(os.environ.get("SDV_MLB_SLEEP", "0"))


class TransportError(RuntimeError):
    """A fetch that could not be completed. Never returned as empty data."""


def _renviron() -> dict:
    """Read credentials at call time. Never cached to disk, never logged."""
    out: dict[str, str] = {}
    for candidate in (
        pathlib.Path.home() / ".Renviron",
        pathlib.Path.home() / "Documents" / ".Renviron",
    ):
        if not candidate.exists():
            continue
        for line in candidate.read_text(errors="replace").splitlines():
            if "=" not in line or line.lstrip().startswith("#"):
                continue
            k, _, v = line.partition("=")
            out.setdefault(k.strip(), v.strip().strip('"').strip("'"))
        break
    return out


_pool_lock = threading.Lock()
_pool: "list[str] | None" = None


def proxy_pool() -> "list[str]":
    """Resolve the ProxyBonanza pool once per process.

    The key is ``PROXY_KEY`` in .Renviron on this box, NOT
    ``PROXYBONANZA_API_KEY`` -- the NCAA hoops launchers read the latter in
    their fallback path and would fail here.
    """
    global _pool
    with _pool_lock:
        if _pool is not None:
            return _pool
        env = _renviron()
        key = os.environ.get("PROXY_KEY") or env.get("PROXY_KEY")
        pkg = os.environ.get("PROXY_PKG") or env.get("PROXY_PKG")
        if not key or not pkg:
            raise TransportError(
                "statsapi.mlb.com returns 406 to this host directly; a proxy is "
                "required. Set PROXY_KEY and PROXY_PKG (ProxyBonanza) in "
                "~/.Renviron or the environment. Note the key is PROXY_KEY here, "
                "not PROXYBONANZA_API_KEY."
            )
        from sportsdataverse.mbb.mbb_ncaa_fetch import load_proxybonanza_pool

        _pool = load_proxybonanza_pool(key, pkg)
        if not _pool:
            raise TransportError("ProxyBonanza returned an empty pool")
        return _pool


def _pick(i: int) -> str:
    pool = proxy_pool()
    return pool[i % len(pool)]


def get(url: str, *, proxied: bool = True, worker: int = 0) -> bytes:
    """Fetch a URL, retrying transient failures. Raises rather than returning empty.

    An empty return would be indistinguishable from "the provider has no data",
    which is how green-but-empty pipeline failures start.
    """
    from curl_cffi import requests as creq

    last = "no attempt made"
    for attempt in range(RETRIES):
        if SLEEP:
            time.sleep(SLEEP)
        proxies = None
        if proxied:
            p = _pick(worker + attempt)
            proxies = {"http": p, "https": p}
        try:
            r = creq.get(url, proxies=proxies, timeout=TIMEOUT, impersonate="chrome")
        except Exception as exc:  # noqa: BLE001 - any transport failure is retryable
            last = f"{type(exc).__name__}: {str(exc)[:120]}"
        else:
            if r.status_code == 200:
                return r.content
            if r.status_code == 404:
                raise TransportError(f"404 (no such resource): {url}")
            last = f"HTTP {r.status_code}"
            if r.status_code == 406 and not proxied:
                raise TransportError(
                    f"HTTP 406 fetching {url} WITHOUT a proxy. statsapi refuses "
                    "this host's IP directly; use proxied=True."
                )
        # jittered backoff: a fixed sleep synchronises every worker onto the
        # same retry instant, which is exactly when the host is least happy.
        time.sleep(min(2**attempt, 8) * (0.5 + random.random()))  # noqa: S311
    raise TransportError(f"{url} failed after {RETRIES} attempts: {last}")


def get_json(url: str, *, proxied: bool = True, worker: int = 0):
    import json

    return json.loads(get(url, proxied=proxied, worker=worker))
