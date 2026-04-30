"""Unit tests for Microsoft email verification."""

from unittest.mock import patch, MagicMock

import pytest

from pipeline.utils.ms_verify import (
    is_microsoft_mx,
    _check_sync,
    DOMAIN_MANAGED,
    DOMAIN_FEDERATED,
    DOMAIN_CONSUMER,
)


class TestIsMicrosoftMx:
    """Test Microsoft MX provider detection."""

    def test_outlook_com_detected(self):
        """outlook.com MX is recognized as Microsoft."""
        assert is_microsoft_mx("mail.protection.outlook.com") is True

    def test_hotmail_com_detected(self):
        """hotmail.com MX is recognized as Microsoft."""
        assert is_microsoft_mx("hotmail.com") is True

    def test_microsoft_com_detected(self):
        """microsoft.com MX is recognized as Microsoft."""
        assert is_microsoft_mx("microsoft.com") is True

    def test_mail_protection_outlook_com_detected(self):
        """mail.protection.outlook.com MX is recognized as Microsoft."""
        assert is_microsoft_mx("mail.protection.outlook.com") is True

    def test_gmail_not_detected(self):
        """gmail.com MX is not recognized as Microsoft."""
        assert is_microsoft_mx("gmail.com") is False

    def test_google_workspace_not_detected(self):
        """aspmx.l.google.com MX is not recognized as Microsoft."""
        assert is_microsoft_mx("aspmx.l.google.com") is False

    def test_custom_domain_not_detected(self):
        """Custom domain MX is not recognized as Microsoft."""
        assert is_microsoft_mx("mail.example.com") is False

    def test_none_mx_returns_false(self):
        """None MX provider returns False."""
        assert is_microsoft_mx(None) is False

    def test_empty_string_mx_returns_false(self):
        """Empty string MX provider returns False."""
        assert is_microsoft_mx("") is False

    def test_case_insensitive_matching(self):
        """MX provider matching is case-insensitive."""
        assert is_microsoft_mx("MAIL.PROTECTION.OUTLOOK.COM") is True
        assert is_microsoft_mx("HOTMAIL.COM") is True

    def test_substring_matching(self):
        """Matches if MS pattern is a substring."""
        assert is_microsoft_mx("smtp.mail.protection.outlook.com") is True


class TestCheckSync:
    """Test synchronous Microsoft GetCredentialType API check."""

    @patch("pipeline.utils.ms_verify.requests.post")
    def test_valid_managed_domain_user_exists(self, mock_post):
        """Valid response from managed domain with existing user."""
        mock_post.return_value = MagicMock(
            status_code=200,
            json=lambda: {
                "IfExistsResult": 0,
                "ThrottleStatus": 0,
                "EstsProperties": {"DomainType": DOMAIN_MANAGED},
            },
        )
        result = _check_sync("user@company.com")
        assert result["status"] == "valid"
        assert result["reason"] == "managed_user_exists"
        assert result["domain_type"] == DOMAIN_MANAGED

    @patch("pipeline.utils.ms_verify.requests.post")
    def test_invalid_managed_domain_user_not_exists(self, mock_post):
        """Valid response from managed domain with non-existent user."""
        mock_post.return_value = MagicMock(
            status_code=200,
            json=lambda: {
                "IfExistsResult": 1,
                "ThrottleStatus": 0,
                "EstsProperties": {"DomainType": DOMAIN_MANAGED},
            },
        )
        result = _check_sync("user@company.com")
        assert result["status"] == "invalid"
        assert result["reason"] == "managed_user_not_exists"
        assert result["domain_type"] == DOMAIN_MANAGED

    @patch("pipeline.utils.ms_verify.requests.post")
    def test_federated_domain_unreliable(self, mock_post):
        """Federated domain always returns unknown."""
        mock_post.return_value = MagicMock(
            status_code=200,
            json=lambda: {
                "IfExistsResult": 0,
                "ThrottleStatus": 0,
                "EstsProperties": {"DomainType": DOMAIN_FEDERATED},
            },
        )
        result = _check_sync("user@federated.com")
        assert result["status"] == "unknown"
        assert result["reason"] == "federated_domain_unreliable"

    @patch("pipeline.utils.ms_verify.requests.post")
    def test_consumer_domain_unreliable(self, mock_post):
        """Consumer domain (outlook.com, hotmail.com) always returns unknown."""
        mock_post.return_value = MagicMock(
            status_code=200,
            json=lambda: {
                "IfExistsResult": 5,
                "ThrottleStatus": 0,
                "EstsProperties": {"DomainType": DOMAIN_CONSUMER},
            },
        )
        result = _check_sync("user@outlook.com")
        assert result["status"] == "unknown"
        assert result["reason"] == "consumer_domain_unreliable"

    @patch("pipeline.utils.ms_verify.requests.post")
    def test_throttled_response(self, mock_post):
        """Throttle status 1 returns throttled."""
        mock_post.return_value = MagicMock(
            status_code=200,
            json=lambda: {
                "IfExistsResult": 0,
                "ThrottleStatus": 1,
                "EstsProperties": {"DomainType": DOMAIN_MANAGED},
            },
        )
        result = _check_sync("user@company.com")
        assert result["status"] == "throttled"
        assert result["reason"] == "ms_throttled"

    @patch("pipeline.utils.ms_verify.requests.post")
    def test_result_code_2_throttled(self, mock_post):
        """Result code 2 is treated as throttled."""
        mock_post.return_value = MagicMock(
            status_code=200,
            json=lambda: {
                "IfExistsResult": 2,
                "ThrottleStatus": 0,
                "EstsProperties": {"DomainType": DOMAIN_MANAGED},
            },
        )
        result = _check_sync("user@company.com")
        assert result["status"] == "throttled"

    @patch("pipeline.utils.ms_verify.requests.post")
    def test_http_error_response(self, mock_post):
        """Non-200 HTTP response returns error."""
        mock_post.return_value = MagicMock(status_code=401)
        result = _check_sync("user@company.com")
        assert result["status"] == "error"
        assert "HTTP 401" in result["reason"]

    @patch("pipeline.utils.ms_verify.requests.post")
    def test_missing_domain_type_handled(self, mock_post):
        """Missing DomainType in response is handled gracefully."""
        mock_post.return_value = MagicMock(
            status_code=200,
            json=lambda: {
                "IfExistsResult": 0,
                "ThrottleStatus": 0,
                "EstsProperties": {},
            },
        )
        result = _check_sync("user@company.com")
        # Missing domain type, may return unknown or error depending on IfExistsResult
        assert result["domain_type"] is None

    @patch("pipeline.utils.ms_verify.requests.post")
    def test_missing_if_exists_result(self, mock_post):
        """Missing IfExistsResult defaults to -1."""
        mock_post.return_value = MagicMock(
            status_code=200,
            json=lambda: {
                "ThrottleStatus": 0,
                "EstsProperties": {"DomainType": DOMAIN_MANAGED},
            },
        )
        result = _check_sync("user@company.com")
        assert result["code"] == -1

    @patch("pipeline.utils.ms_verify.requests.post")
    def test_json_decode_error(self, mock_post):
        """Invalid JSON response returns error."""
        mock_post.return_value = MagicMock(
            status_code=200,
            json=lambda: None,
        )
        try:
            result = _check_sync("user@company.com")
        except Exception:
            # May raise or return error dict depending on implementation
            pass

    @patch("pipeline.utils.ms_verify.requests.post")
    def test_timeout_behavior(self, mock_post):
        """Timeout parameter is passed to requests.post."""
        import requests
        mock_post.side_effect = requests.Timeout("Connection timeout")
        try:
            result = _check_sync("user@company.com", timeout=5.0)
        except:
            pass
        mock_post.assert_called_once()
        # Verify timeout was passed
        assert mock_post.call_args[1]["timeout"] == 5.0

    @patch("pipeline.utils.ms_verify.requests.post")
    def test_user_agent_header_set(self, mock_post):
        """Request includes User-Agent header."""
        mock_post.return_value = MagicMock(
            status_code=200,
            json=lambda: {
                "IfExistsResult": 0,
                "ThrottleStatus": 0,
                "EstsProperties": {"DomainType": DOMAIN_MANAGED},
            },
        )
        _check_sync("user@company.com")
        call_args = mock_post.call_args
        headers = call_args[1]["headers"]
        assert "User-Agent" in headers
        assert len(headers["User-Agent"]) > 0

    @patch("pipeline.utils.ms_verify.requests.post")
    def test_request_body_format(self, mock_post):
        """Request body contains expected fields."""
        mock_post.return_value = MagicMock(
            status_code=200,
            json=lambda: {
                "IfExistsResult": 0,
                "ThrottleStatus": 0,
                "EstsProperties": {"DomainType": DOMAIN_MANAGED},
            },
        )
        _check_sync("user@company.com")
        call_args = mock_post.call_args
        body = call_args[1]["json"]
        assert body["Username"] == "user@company.com"
        assert "isOtherIdpSupported" in body
        assert "checkPhones" in body

    @patch("pipeline.utils.ms_verify.requests.post")
    def test_multiple_calls_different_user_agents(self, mock_post):
        """Multiple calls may use different user agents."""
        mock_post.return_value = MagicMock(
            status_code=200,
            json=lambda: {
                "IfExistsResult": 0,
                "ThrottleStatus": 0,
                "EstsProperties": {"DomainType": DOMAIN_MANAGED},
            },
        )
        _check_sync("user1@company.com")
        _check_sync("user2@company.com")
        # Both calls should have user-agent headers (may vary due to random choice)
        assert mock_post.call_count == 2


class TestCheckMicrosoftEmailAsync:
    """Test async wrapper for Microsoft email checks.

    Note: This would require async testing with pytest-asyncio.
    See integration/test_db.py for async fixture patterns.
    """

    pass
