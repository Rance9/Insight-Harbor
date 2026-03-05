"""
Tests for pipeline.shared.graph_client
"""

from unittest.mock import MagicMock, patch

import pytest

from pipeline.shared.graph_client import GraphClient, PermanentError, ThrottledError


# ═══════════════════════════════════════════════════════════════════════════════
# Token Acquisition
# ═══════════════════════════════════════════════════════════════════════════════


class TestGraphClientToken:
    @patch("pipeline.shared.graph_client.msal.ConfidentialClientApplication")
    def test_successful_token_acquisition(self, mock_msal_cls):
        mock_app = MagicMock()
        mock_app.acquire_token_for_client.return_value = {
            "access_token": "test-token-123",
            "expires_in": 3600,
        }
        mock_msal_cls.return_value = mock_app

        client = GraphClient()
        token = client._get_token()
        assert token == "test-token-123"

    @patch("pipeline.shared.graph_client.msal.ConfidentialClientApplication")
    def test_failed_token_raises(self, mock_msal_cls):
        mock_app = MagicMock()
        mock_app.acquire_token_for_client.return_value = {
            "error": "invalid_client",
            "error_description": "Bad credentials",
        }
        mock_msal_cls.return_value = mock_app

        client = GraphClient()
        with pytest.raises(PermanentError, match="Bad credentials"):
            client._get_token()


# ═══════════════════════════════════════════════════════════════════════════════
# Response Handling
# ═══════════════════════════════════════════════════════════════════════════════


class TestGraphClientResponseHandling:
    @patch("pipeline.shared.graph_client.msal.ConfidentialClientApplication")
    def test_429_raises_throttled(self, mock_msal_cls):
        mock_app = MagicMock()
        mock_app.acquire_token_for_client.return_value = {"access_token": "tok"}
        mock_msal_cls.return_value = mock_app

        client = GraphClient()

        mock_response = MagicMock()
        mock_response.status_code = 429
        mock_response.headers = {"Retry-After": "60"}

        with pytest.raises(ThrottledError) as exc_info:
            client._handle_response(mock_response)

        assert exc_info.value.retry_after_seconds == 60

    @patch("pipeline.shared.graph_client.msal.ConfidentialClientApplication")
    def test_403_raises_permanent(self, mock_msal_cls):
        mock_app = MagicMock()
        mock_app.acquire_token_for_client.return_value = {"access_token": "tok"}
        mock_msal_cls.return_value = mock_app

        client = GraphClient()

        mock_response = MagicMock()
        mock_response.status_code = 403
        mock_response.text = "Forbidden"

        with pytest.raises(PermanentError):
            client._handle_response(mock_response)

    @patch("pipeline.shared.graph_client.msal.ConfidentialClientApplication")
    def test_500_raises_runtime(self, mock_msal_cls):
        mock_app = MagicMock()
        mock_app.acquire_token_for_client.return_value = {"access_token": "tok"}
        mock_msal_cls.return_value = mock_app

        client = GraphClient()

        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_response.text = "Internal Server Error"

        with pytest.raises(RuntimeError):
            client._handle_response(mock_response)

    @patch("pipeline.shared.graph_client.msal.ConfidentialClientApplication")
    def test_200_succeeds(self, mock_msal_cls):
        mock_app = MagicMock()
        mock_app.acquire_token_for_client.return_value = {"access_token": "tok"}
        mock_msal_cls.return_value = mock_app

        client = GraphClient()

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"value": []}

        result = client._handle_response(mock_response)
        assert result == {"value": []}


# ═══════════════════════════════════════════════════════════════════════════════
# Custom Exceptions
# ═══════════════════════════════════════════════════════════════════════════════


class TestCustomExceptions:
    def test_throttled_error_attributes(self):
        err = ThrottledError("Throttled", retry_after_seconds=120)
        assert err.retry_after_seconds == 120
        assert "Throttled" in str(err)

    def test_permanent_error(self):
        err = PermanentError("Forbidden")
        assert "Forbidden" in str(err)
