"""Shared helpers: request signing (server -> agent) and a tiny HTTP client."""
import hashlib
import hmac
import secrets
import threading
import time
from datetime import datetime, timezone

import requests

HEADER_TS = "X-Relay-Timestamp"
HEADER_NONCE = "X-Relay-Nonce"
HEADER_SIG = "X-Relay-Signature"
MAX_SKEW_SECONDS = 60


def utcnow():
    """Naive UTC datetime (what we store in the database)."""
    return datetime.now(timezone.utc).replace(tzinfo=None)


def sha256_hex(data):
    return hashlib.sha256(data).hexdigest()


def _message(method, path, ts, nonce, body):
    return f"{method.upper()}\n{path}\n{ts}\n{nonce}\n{sha256_hex(body)}".encode()


def sign_headers(secret, method, path, body=b"", now=None):
    """Headers proving the request comes from someone who knows ``secret``."""
    ts = str(int(now if now is not None else time.time()))
    nonce = secrets.token_hex(12)
    sig = hmac.new(secret.encode(), _message(method, path, ts, nonce, body), hashlib.sha256)
    return {HEADER_TS: ts, HEADER_NONCE: nonce, HEADER_SIG: sig.hexdigest()}


class SignatureError(Exception):
    pass


class Verifier:
    """Checks HMAC signatures, rejecting stale timestamps and replayed nonces."""

    def __init__(self, secret, max_skew=MAX_SKEW_SECONDS):
        self._secret = secret
        self._max_skew = max_skew
        self._seen = {}
        self._lock = threading.Lock()

    def verify(self, method, path, body, headers, now=None):
        now = now if now is not None else time.time()
        ts = headers.get(HEADER_TS)
        nonce = headers.get(HEADER_NONCE)
        sig = headers.get(HEADER_SIG)
        if not (ts and nonce and sig):
            raise SignatureError("missing signature headers")
        try:
            skew = abs(now - int(ts))
        except ValueError:
            raise SignatureError("bad timestamp")
        if skew > self._max_skew:
            raise SignatureError("timestamp outside allowed window")
        expected = hmac.new(
            self._secret.encode(), _message(method, path, ts, nonce, body), hashlib.sha256
        ).hexdigest()
        if not hmac.compare_digest(expected, sig):
            raise SignatureError("bad signature")
        with self._lock:
            cutoff = now - 2 * self._max_skew
            self._seen = {n: t for n, t in self._seen.items() if t > cutoff}
            if nonce in self._seen:
                raise SignatureError("replayed request")
            self._seen[nonce] = now


class HttpClient:
    """Thin wrapper over ``requests`` so tests can swap in a fake."""

    def get(self, url, headers=None, timeout=5):
        return requests.get(url, headers=headers, timeout=timeout)

    def post(self, url, body=b"", headers=None, timeout=10):
        return requests.post(url, data=body, headers=headers, timeout=timeout)
