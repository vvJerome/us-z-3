"""Minimal S3-compatible (Cloudflare R2) PutObject over AWS SigV4.

Dependency-free: signs requests with hmac/hashlib and uploads via the shared aiohttp
session — no boto3. R2 uses region "auto". The endpoint already includes the bucket
(e.g. https://<acct>.r2.cloudflarestorage.com/<bucket>); object keys are appended.
Credentials come from the environment (R2_ACCESS_KEY_ID / R2_SECRET_ACCESS_KEY).
"""
from __future__ import annotations

import datetime
import hashlib
import hmac
import logging
from urllib.parse import urlsplit

import aiohttp

logger = logging.getLogger("pipeline.storage.r2")

_ALGORITHM = "AWS4-HMAC-SHA256"


def signing_key(secret: str, datestamp: str, region: str, service: str) -> bytes:
    """Derive the SigV4 signing key (AWS-documented HMAC chain)."""
    k_date = hmac.new(("AWS4" + secret).encode(), datestamp.encode(), hashlib.sha256).digest()
    k_region = hmac.new(k_date, region.encode(), hashlib.sha256).digest()
    k_service = hmac.new(k_region, service.encode(), hashlib.sha256).digest()
    return hmac.new(k_service, b"aws4_request", hashlib.sha256).digest()


class R2Client:
    def __init__(
        self,
        endpoint: str,
        access_key: str,
        secret_key: str,
        *,
        region: str = "auto",
        service: str = "s3",
        session: aiohttp.ClientSession | None = None,
    ) -> None:
        self.endpoint = endpoint.rstrip("/")
        self.access_key = access_key
        self.secret_key = secret_key
        self.region = region
        self.service = service
        self._session = session

    def _auth_headers(self, method: str, url: str, payload: bytes, now: datetime.datetime) -> dict[str, str]:
        parsed = urlsplit(url)
        host = parsed.netloc
        amz_date = now.strftime("%Y%m%dT%H%M%SZ")
        datestamp = now.strftime("%Y%m%d")
        payload_hash = hashlib.sha256(payload).hexdigest()
        signed_headers = "host;x-amz-content-sha256;x-amz-date"
        canonical_headers = f"host:{host}\nx-amz-content-sha256:{payload_hash}\nx-amz-date:{amz_date}\n"
        canonical_request = "\n".join(
            [method, parsed.path, "", canonical_headers, signed_headers, payload_hash]
        )
        scope = f"{datestamp}/{self.region}/{self.service}/aws4_request"
        string_to_sign = "\n".join(
            [_ALGORITHM, amz_date, scope, hashlib.sha256(canonical_request.encode()).hexdigest()]
        )
        signature = hmac.new(
            signing_key(self.secret_key, datestamp, self.region, self.service),
            string_to_sign.encode(), hashlib.sha256,
        ).hexdigest()
        authorization = (
            f"{_ALGORITHM} Credential={self.access_key}/{scope}, "
            f"SignedHeaders={signed_headers}, Signature={signature}"
        )
        return {
            "Authorization": authorization,
            "x-amz-content-sha256": payload_hash,
            "x-amz-date": amz_date,
            "Host": host,
        }

    async def put_object(self, key: str, data: bytes, now: datetime.datetime | None = None) -> None:
        """Upload `data` to `<endpoint>/<key>`. Raises on a non-2xx response."""
        if self._session is None:
            raise RuntimeError("R2Client requires an aiohttp session")
        now = now or datetime.datetime.now(datetime.timezone.utc)
        url = f"{self.endpoint}/{key.lstrip('/')}"
        headers = self._auth_headers("PUT", url, data, now)
        async with self._session.put(url, data=data, headers=headers) as resp:
            if resp.status >= 300:
                body = await resp.text()
                raise RuntimeError(f"R2 PUT {key} failed: {resp.status} {body[:200]}")
        logger.info("uploaded %s to R2 (%d bytes)", key, len(data))
