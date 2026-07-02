"""Unit tests for MX-host → canonical mail-provider classification."""

import pytest

from pipeline.utils.providers import classify_provider


@pytest.mark.parametrize(
    "mx, expected",
    [
        ("company-com.mail.protection.outlook.com", "microsoft"),
        ("smtp.mail.protection.outlook.com", "microsoft"),
        ("OUTLOOK.COM", "microsoft"),
        ("hotmail.com", "microsoft"),
        ("microsoft.com", "microsoft"),
        ("aspmx.l.google.com", "google"),
        ("alt1.aspmx.l.google.com", "google"),
        ("gmail.com", "google"),
        ("googlemail.com", "google"),
        ("mta5.am0.yahoodns.net", "yahoo"),
        ("mx.zoho.com", "zoho"),
        ("mxa-00.pphosted.com", "proofpoint"),
        ("us-smtp-inbound-1.mimecast.com", "mimecast"),
        ("cudamail.barracudanetworks.com", "barracuda"),
        ("mx01.mail.icloud.com", "icloud"),
        ("inbound-smtp.us-east-1.amazonaws.com", "amazon"),
        ("smtp.secureserver.net", "secureserver"),
        ("mx.yandex.net", "yandex"),
        ("mail.example.com", "other"),
        ("", "other"),
        (None, "other"),
    ],
)
def test_classify_provider(mx, expected):
    assert classify_provider(mx) == expected
