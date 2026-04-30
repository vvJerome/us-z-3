"""Microsoft 365 email verification via GetCredentialType API.

Public, undocumented API used by Microsoft's login flow. No auth required,
no password attempted, no alerts to target.

Reliability depends on DomainType:
  DomainType=3 (managed):   code 0 = user exists, code 1 = doesn't → RELIABLE
  DomainType=4 (federated): always returns code 0 + ThrottleStatus=1 → UNRELIABLE
  DomainType=2 (consumer):  always returns code 5 for any email → UNRELIABLE

We only trust results from DomainType=3 (managed) domains.
"""

from __future__ import annotations

import asyncio
import logging
import random

import requests

logger = logging.getLogger("pipeline.consumer")

MS_CREDENTIAL_URL = "https://login.microsoftonline.com/common/GetCredentialType?mkt=en-US"

DOMAIN_MANAGED = 3    # Azure AD managed — IfExistsResult is reliable
DOMAIN_FEDERATED = 4  # Federated IdP — always returns code 0, unreliable
DOMAIN_CONSUMER = 2   # Consumer (outlook.com, hotmail.com) — code 5 for everything

_USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
]

# MX host substrings that indicate a Microsoft-hosted domain
MS_MX_PATTERNS = (
    "mail.protection.outlook.com",
    "outlook.com",
    "hotmail.com",
    "microsoft.com",
)


def is_microsoft_mx(mx_provider: str | None) -> bool:
    """True if the MX host indicates Microsoft 365 or Exchange Online."""
    if not mx_provider:
        return False
    lp = mx_provider.lower()
    return any(p in lp for p in MS_MX_PATTERNS)


def _check_sync(email: str, timeout: float = 10.0) -> dict:
    try:
        headers = {
            "User-Agent": random.choice(_USER_AGENTS),
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        body = {
            "Username": email,
            "isOtherIdpSupported": True,
            "checkPhones": False,
            "isRemoteNGCSupported": True,
            "isCookieBannerShown": False,
            "isFidoSupported": False,
            "isSignup": False,
            "isAccessPassSupported": True,
            "isRemoteConnectSupported": False,
            "federationFlags": 0,
            "forceotclogin": False,
        }

        resp = requests.post(MS_CREDENTIAL_URL, json=body, headers=headers, timeout=timeout)

        if resp.status_code != 200:
            return {"status": "error", "code": -1, "reason": f"HTTP {resp.status_code}"}

        data = resp.json()
        result_code = data.get("IfExistsResult", -1)
        throttle_status = data.get("ThrottleStatus", 0)
        domain_type = data.get("EstsProperties", {}).get("DomainType")

        base = {"code": result_code, "domain_type": domain_type}

        if result_code == 2 or throttle_status == 1:
            return {**base, "status": "throttled", "reason": "ms_throttled"}

        if domain_type == DOMAIN_FEDERATED:
            return {**base, "status": "unknown", "reason": "federated_domain_unreliable"}

        if domain_type == DOMAIN_CONSUMER:
            return {**base, "status": "unknown", "reason": "consumer_domain_unreliable"}

        if domain_type == DOMAIN_MANAGED:
            if result_code == 0:
                return {**base, "status": "valid", "reason": "managed_user_exists"}
            elif result_code == 1:
                return {**base, "status": "invalid", "reason": "managed_user_not_exists"}
            else:
                return {**base, "status": "unknown", "reason": f"managed_unknown_code_{result_code}"}

        if result_code == 1:
            return {**base, "status": "invalid", "reason": "ms_not_exists"}
        return {**base, "status": "unknown", "reason": f"unknown_domain_type_{domain_type}"}

    except requests.exceptions.Timeout:
        return {"status": "error", "code": -1, "reason": "ms_timeout"}
    except Exception as exc:
        return {"status": "error", "code": -1, "reason": f"ms_error: {exc}"}


async def check_microsoft_email_async(email: str, timeout: float = 10.0) -> dict:
    """Async wrapper — runs sync HTTP call in thread pool."""
    return await asyncio.to_thread(_check_sync, email, timeout)
