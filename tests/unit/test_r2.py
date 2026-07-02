"""Unit tests for the dependency-free R2/S3 SigV4 uploader."""

import datetime
import hashlib

import pytest

from pipeline.storage.r2 import R2Client, signing_key


def test_signing_key_deterministic_and_32_bytes():
    a = signing_key("secret", "20260622", "auto", "s3")
    b = signing_key("secret", "20260622", "auto", "s3")
    assert a == b
    assert len(a) == 32


def test_signing_key_changes_with_secret():
    assert signing_key("s1", "20260622", "auto", "s3") != signing_key("s2", "20260622", "auto", "s3")


class _FakeResp:
    def __init__(self, status):
        self.status = status

    async def text(self):
        return "denied" if self.status >= 300 else ""

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False


class _FakeSession:
    def __init__(self, status=200):
        self.status = status
        self.calls = []

    def put(self, url, data=None, headers=None):
        self.calls.append({"url": url, "data": data, "headers": headers})
        return _FakeResp(self.status)


async def test_put_object_signs_and_uploads():
    sess = _FakeSession(200)
    client = R2Client("https://acct.r2.cloudflarestorage.com/bucket", "AK", "SK", session=sess)
    now = datetime.datetime(2026, 6, 22, 12, 0, 0, tzinfo=datetime.timezone.utc)
    await client.put_object("pipeline.db", b"data", now=now)
    call = sess.calls[0]
    assert call["url"] == "https://acct.r2.cloudflarestorage.com/bucket/pipeline.db"
    assert call["headers"]["Authorization"].startswith(
        "AWS4-HMAC-SHA256 Credential=AK/20260622/auto/s3/aws4_request"
    )
    assert "SignedHeaders=host;x-amz-content-sha256;x-amz-date" in call["headers"]["Authorization"]
    assert call["headers"]["x-amz-content-sha256"] == hashlib.sha256(b"data").hexdigest()


async def test_put_object_raises_on_error_status():
    client = R2Client("https://acct.r2.cloudflarestorage.com/bucket", "AK", "SK", session=_FakeSession(403))
    with pytest.raises(RuntimeError):
        await client.put_object("k", b"x")


async def test_put_object_without_session_raises():
    client = R2Client("https://acct.r2.cloudflarestorage.com/bucket", "AK", "SK")
    with pytest.raises(RuntimeError):
        await client.put_object("k", b"x")
