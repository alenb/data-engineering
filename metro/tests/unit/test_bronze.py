"""
Unit tests for the Bronze layer data ingestion.

These tests validate the Bronze layer's ability to fetch data from APIs
and handle various response scenarios. They use mocking to avoid making
real API calls during testing.
"""

import pytest
import requests
from unittest.mock import patch, Mock, call
from scripts.bronze import Bronze


class TestBronze:
    """Test class for Bronze layer functionality."""

    @pytest.mark.unit
    def test_init(self, mock_config):
        """Test Bronze class initialization."""
        with patch('scripts.base.Config', return_value=mock_config):
            bronze = Bronze()
            assert bronze.config == mock_config
            assert hasattr(bronze, 'logger')
            assert hasattr(bronze, 'spark')

    @pytest.mark.unit
    @patch('scripts.bronze.requests.get')
    @patch.object(Bronze, 'download')
    def test_get_train_patronage_success(self, mock_download, mock_requests_get, bronze_processor):
        """
        Test successful train patronage data retrieval.
        
        This test mocks the API response and verifies that the Bronze
        processor correctly handles a successful API call and initiates download.
        """
        # Mock successful API response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "result": {
                "url": "https://example.com/data/train_patrons.csv",
                "format": "CSV"
            }
        }
        mock_requests_get.return_value = mock_response
        
        # Mock successful download
        mock_download.return_value = None
        
        # Execute the method
        bronze_processor.get_train_patronage()
        
        # Verify API call was made with correct parameters
        expected_endpoint = f"{bronze_processor.config.DATAVIC_BASE_URL}/resource_show?id=162887ef-1dba-4d9b-83bd-baee229229c6"
        expected_headers = {
            "accept": "application/json", 
            "apikey": bronze_processor.config.DATAVIC_API_KEY
        }
        
        mock_requests_get.assert_called_once_with(
            expected_endpoint, 
            headers=expected_headers, 
            timeout=30
        )
        
        # Verify download was initiated with correct parameters
        mock_download.assert_called_once_with(
            "https://example.com/data/train_patrons.csv",
            "train_patrons.csv"
        )

    @pytest.mark.unit
    @patch('scripts.bronze.requests.get')
    def test_get_train_patronage_api_error(self, mock_requests_get, bronze_processor):
        """
        Test handling of API error responses.
        
        This test verifies that the Bronze processor properly handles
        HTTP error responses from the API.
        """
        # Mock API error response
        mock_response = Mock()
        mock_response.status_code = 404
        mock_requests_get.return_value = mock_response
        
        # Execute and verify exception is raised
        with pytest.raises(ValueError, match="Train service passenger counts data could not be loaded"):
            bronze_processor.get_train_patronage()

    @pytest.mark.unit
    @patch('scripts.bronze.requests.get')
    def test_get_train_patronage_missing_url(self, mock_requests_get, bronze_processor):
        """
        Test handling of API response missing download URL.
        
        This test verifies proper error handling when the API response
        doesn't contain the expected download URL.
        """
        # Mock API response without download URL
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "result": {
                "format": "CSV"
                # Missing "url" field
            }
        }
        mock_requests_get.return_value = mock_response
        
        # Execute and verify exception is raised
        with pytest.raises(ValueError, match="Train service passenger counts download url not found"):
            bronze_processor.get_train_patronage()

    @pytest.mark.unit
    @patch('scripts.bronze.requests.get')
    def test_get_train_patronage_empty_result(self, mock_requests_get, bronze_processor):
        """
        Test handling of API response with empty result.
        
        This test verifies error handling when the API returns
        a successful response but with empty or malformed data.
        """
        # Mock API response with empty result
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "result": {}
        }
        mock_requests_get.return_value = mock_response
        
        # Execute and verify exception is raised
        with pytest.raises(ValueError, match="Train service passenger counts download url not found"):
            bronze_processor.get_train_patronage()

    @pytest.mark.unit
    @patch('scripts.bronze.requests.get')
    @patch.object(Bronze, 'download')
    def test_get_train_patronage_download_failure(self, mock_download, mock_requests_get, bronze_processor):
        """
        Test handling of download failures.
        
        This test verifies that download errors are properly propagated
        and logged when the file download fails.
        """
        # Mock successful API response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "result": {
                "url": "https://example.com/data/train_patrons.csv",
                "format": "CSV"
            }
        }
        mock_requests_get.return_value = mock_response
        
        # Mock download failure
        mock_download.side_effect = Exception("Network error during download")
        
        # Execute and verify exception is raised
        with pytest.raises(Exception, match="Network error during download"):
            bronze_processor.get_train_patronage()

    @pytest.mark.unit
    @patch('scripts.bronze.requests.get')
    @patch.object(Bronze, 'download')
    def test_get_train_patronage_different_formats(self, mock_download, mock_requests_get, bronze_processor):
        """
        Test handling of different file formats.
        
        This test verifies that the Bronze processor correctly handles
        different file formats returned by the API.
        """
        test_cases = [
            ("CSV", "train_patrons.csv"),
            ("JSON", "train_patrons.json"),
            ("XLSX", "train_patrons.xlsx"),
            (None, "train_patrons.csv"),  # Default to CSV when format is missing
        ]
        
        for api_format, expected_filename in test_cases:
            # Reset mocks
            mock_requests_get.reset_mock()
            mock_download.reset_mock()
            
            # Mock API response with specific format
            response_data = {
                "result": {
                    "url": f"https://example.com/data/train_patrons.{api_format.lower() if api_format else 'csv'}"
                }
            }
            if api_format:
                response_data["result"]["format"] = api_format
            
            mock_response = Mock()
            mock_response.status_code = 200
            mock_response.json.return_value = response_data
            mock_requests_get.return_value = mock_response
            
            # Execute the method
            bronze_processor.get_train_patronage()
            
            # Verify correct filename is used
            mock_download.assert_called_once()
            call_args = mock_download.call_args[0]
            assert call_args[1] == expected_filename

    @pytest.mark.unit
    @patch('scripts.bronze.requests.get')
    def test_get_train_patronage_request_timeout(self, mock_requests_get, bronze_processor):
        """
        Test handling of request timeouts.
        
        This test verifies that network timeouts are properly handled
        and result in appropriate error responses.
        """
        # Mock timeout exception
        mock_requests_get.side_effect = requests.exceptions.Timeout("Request timed out")
        
        # Execute and verify exception is raised
        with pytest.raises(requests.exceptions.Timeout):
            bronze_processor.get_train_patronage()

    @pytest.mark.unit
    @patch('scripts.bronze.requests.get')
    def test_get_train_patronage_connection_error(self, mock_requests_get, bronze_processor):
        """
        Test handling of connection errors.
        
        This test verifies that network connection errors are properly
        handled and propagated.
        """
        # Mock connection error
        mock_requests_get.side_effect = requests.exceptions.ConnectionError("Failed to connect")
        
        # Execute and verify exception is raised
        with pytest.raises(requests.exceptions.ConnectionError):
            bronze_processor.get_train_patronage()

    @pytest.mark.unit
    @patch.object(Bronze, 'get_train_patronage')
    def test_run_method(self, mock_get_train_patronage, bronze_processor):
        """
        Test the run method executes the pipeline correctly.
        
        This test verifies that the main run method calls the
        appropriate data retrieval method.
        """
        # Execute the run method
        bronze_processor.run()
        
        # Verify get_train_patronage was called
        mock_get_train_patronage.assert_called_once()


class TestBronzeIntegration:
    """Integration tests for Bronze layer."""

    @pytest.mark.integration
    @patch('scripts.bronze.requests.get')
    @patch.object(Bronze, 'download')
    def test_complete_bronze_workflow(self, mock_download, mock_requests_get, bronze_processor):
        """
        Integration test for the complete Bronze workflow.
        
        This test runs the entire Bronze processing pipeline to ensure
        all components work together correctly.
        """
        # Mock successful API response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "result": {
                "url": "https://example.com/data/train_patrons.csv",
                "format": "CSV",
                "name": "Train Patronage Data",
                "description": "Daily train patronage statistics"
            }
        }
        mock_requests_get.return_value = mock_response
        mock_download.return_value = None
        
        # Execute the complete workflow
        bronze_processor.run()
        
        # Verify the complete workflow executed
        assert mock_requests_get.called
        assert mock_download.called


# Parameterised tests for API responses
class TestBronzeAPIResponses:
    """Test various API response scenarios."""

    @pytest.mark.parametrise("status_code,should_raise", [
        (200, False),   # Success
        (400, True),    # Bad request
        (401, True),    # Unauthorized
        (404, True),    # Not found
        (500, True),    # Server error
        (503, True),    # Service unavailable
    ])
    @patch('scripts.bronze.requests.get')
    def test_various_http_status_codes(self, mock_requests_get, bronze_processor, status_code, should_raise):
        """
        Parameterised test for different HTTP status codes.
        
        This test verifies proper handling of various HTTP response codes
        that might be returned by the API.
        """
        mock_response = Mock()
        mock_response.status_code = status_code
        mock_requests_get.return_value = mock_response
        
        if status_code == 200:
            mock_response.json.return_value = {
                "result": {
                    "url": "https://example.com/data/train_patrons.csv",
                    "format": "CSV"
                }
            }
            # Also need to mock the download method for successful case
            with patch.object(bronze_processor, 'download'):
                if should_raise:
                    with pytest.raises(ValueError):
                        bronze_processor.get_train_patronage()
                else:
                    bronze_processor.get_train_patronage()  # Should not raise
        else:
            if should_raise:
                with pytest.raises(ValueError, match="Train service passenger counts data could not be loaded"):
                    bronze_processor.get_train_patronage()
            else:
                bronze_processor.get_train_patronage()  # Should not raise
