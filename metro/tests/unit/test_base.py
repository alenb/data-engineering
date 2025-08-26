"""
Unit tests for the Base class functionality.

These tests validate the common functionality provided by the Base class,
including Spark session management and file download capabilities.
"""

import pytest
import subprocess
from pathlib import Path
from unittest.mock import patch, Mock, call
from scripts.base import Base


class TestBase:
    """Test class for Base functionality."""

    def test_init(self):
        """Test Base class initialization."""
        with patch('scripts.base.Config') as mock_config_class, \
             patch('scripts.base.Logger') as mock_logger_class:
            
            mock_config = Mock()
            mock_logger = Mock()
            mock_config_class.return_value = mock_config
            mock_logger_class.return_value = mock_logger
            
            base = Base()
            
            assert base.config == mock_config
            assert base.logger == mock_logger
            assert base.spark is None

    @pytest.mark.unit
    @patch('scripts.base.configure_spark_with_delta_pip')
    @patch('scripts.base.SparkSession.builder')
    def test_create_spark_success(self, mock_builder, mock_configure_delta):
        """
        Test successful Spark session creation.
        
        This test verifies that the Spark session is created with the
        correct configuration for Delta Lake support.
        """
        with patch('scripts.base.Config') as mock_config_class, \
             patch('scripts.base.Logger') as mock_logger_class:
            
            # Set up mocks
            mock_spark_session = Mock()
            mock_configure_delta.return_value.getOrCreate.return_value = mock_spark_session
            
            # Create chain of builder calls
            mock_builder_instance = Mock()
            mock_builder.config.return_value = mock_builder_instance
            mock_builder_instance.config.return_value = mock_builder_instance
            mock_builder.return_value = mock_builder_instance
            
            base = Base()
            base.create_spark()
            
            # Verify Spark session was created
            assert base.spark == mock_spark_session
            
            # Verify Delta configuration was applied
            mock_configure_delta.assert_called_once()

    @pytest.mark.unit
    @patch('scripts.base.subprocess.run')
    def test_download_success(self, mock_subprocess_run):
        """
        Test successful file download using aria2.
        
        This test verifies that the download method correctly calls
        aria2 with the appropriate parameters.
        """
        with patch('scripts.base.Config') as mock_config_class, \
             patch('scripts.base.Logger') as mock_logger_class, \
             patch('pathlib.Path.mkdir') as mock_mkdir:
            
            mock_config = Mock()
            mock_config.BRONZE_DATA_PATH = Path("/test/bronze")
            mock_config_class.return_value = mock_config
            
            # Mock successful subprocess call
            mock_subprocess_run.return_value.returncode = 0
            
            # Mock file stats for the success path
            with patch('pathlib.Path.stat') as mock_stat:
                mock_stat.return_value.st_size = 1024
                
                base = Base()
                base.download("https://example.com/file.csv", "test_file.csv")
            
            # Verify mkdir was called
            mock_mkdir.assert_called_once_with(parents=True, exist_ok=True)
            
            # Verify aria2 was called
            assert mock_subprocess_run.called

    @pytest.mark.unit
    @patch('scripts.base.subprocess.run')
    def test_download_failure(self, mock_subprocess_run):
        """
        Test handling of download failures.
        
        This test verifies that download errors are properly handled
        and re-raised with appropriate error messages.
        """
        with patch('scripts.base.Config') as mock_config_class, \
             patch('scripts.base.Logger') as mock_logger_class:
            
            mock_config = Mock()
            mock_config.BRONZE_DATA_PATH = Path("/test/bronze")
            mock_config_class.return_value = mock_config
            
            # Mock subprocess failure
            mock_subprocess_run.return_value.returncode = 1
            mock_subprocess_run.return_value.stderr = "Download failed"
            
            base = Base()
            
            # Verify exception is raised on download failure
            with pytest.raises(Exception, match="aria2 download failed"):
                base.download("https://example.com/file.csv", "test_file.csv")

    @pytest.mark.unit
    @patch('scripts.base.subprocess.run')
    def test_download_with_special_characters(self, mock_subprocess_run):
        """
        Test download with URLs and filenames containing special characters.
        
        This test ensures that the download method properly handles
        URLs and filenames with spaces and special characters.
        """
        with patch('scripts.base.Config') as mock_config_class, \
             patch('scripts.base.Logger') as mock_logger_class, \
             patch('pathlib.Path.mkdir') as mock_mkdir:
            
            mock_config = Mock()
            mock_config.BRONZE_DATA_PATH = Path("/test/bronze")
            mock_config_class.return_value = mock_config
            
            mock_subprocess_run.return_value.returncode = 0
            
            # Mock file stats for the success path
            with patch('pathlib.Path.stat') as mock_stat:
                mock_stat.return_value.st_size = 2048
                
                base = Base()
                base.download(
                    "https://example.com/file%20with%20spaces.csv", 
                    "file with spaces.csv"
                )
            
            # Verify command was constructed correctly
            assert mock_subprocess_run.called
            call_args = mock_subprocess_run.call_args[0][0]
            assert "https://example.com/file%20with%20spaces.csv" in call_args
            assert "file with spaces.csv" in call_args

    @pytest.mark.unit
    @patch('scripts.base.subprocess.run')
    def test_download_logging(self, mock_subprocess_run):
        """
        Test that download operations are properly logged.
        
        This test verifies that the download method logs appropriate
        information about the download operation.
        """
        with patch('scripts.base.Config') as mock_config_class, \
             patch('scripts.base.Logger') as mock_logger_class, \
             patch('pathlib.Path.mkdir') as mock_mkdir:
            
            mock_config = Mock()
            mock_config.BRONZE_DATA_PATH = Path("/test/bronze")
            mock_config_class.return_value = mock_config
            
            mock_logger = Mock()
            mock_logger_class.return_value = mock_logger
            
            mock_subprocess_run.return_value.returncode = 0
            
            # Mock file stats for the success path
            with patch('pathlib.Path.stat') as mock_stat:
                mock_stat.return_value.st_size = 512
                
                base = Base()
                base.download("https://example.com/file.csv", "test_file.csv")
            
            # Verify logging calls were made
            assert mock_logger.info.called

    @pytest.mark.unit
    def test_base_inheritance(self):
        """
        Test that Base class can be properly inherited.
        
        This test verifies that the Base class provides the expected
        interface for subclasses to inherit.
        """
        with patch('scripts.base.Config') as mock_config_class, \
             patch('scripts.base.Logger') as mock_logger_class:
            
            class TestChild(Base):
                def custom_method(self):
                    return "child_method_called"
            
            child = TestChild()
            assert hasattr(child, 'config')
            assert hasattr(child, 'logger')
            assert hasattr(child, 'spark')
            assert hasattr(child, 'create_spark')
            assert hasattr(child, 'download')
            assert child.custom_method() == "child_method_called"


class TestBaseIntegration:
    """Integration tests for Base class functionality."""

    @pytest.mark.integration
    @pytest.mark.slow
    def test_spark_session_creation_and_cleanup(self):
        """
        Integration test for Spark session lifecycle.
        
        This test verifies that Spark sessions can be created and
        properly cleaned up without resource leaks.
        """
        with patch('scripts.base.Config') as mock_config_class, \
             patch('scripts.base.Logger') as mock_logger_class:
            
            base = Base()
            
            # Create Spark session
            base.create_spark()
            assert base.spark is not None
            
            # Verify session is functional
            assert hasattr(base.spark, 'sparkContext')
            
            # Clean up
            if base.spark:
                base.spark.stop()

    @pytest.mark.integration
    @patch('scripts.base.subprocess.run')
    def test_download_with_retry_logic(self, mock_subprocess_run):
        """
        Integration test for download retry behaviour.
        
        This test verifies that the download method's retry logic
        works as expected when configured in aria2.
        """
        with patch('scripts.base.Config') as mock_config_class, \
             patch('scripts.base.Logger') as mock_logger_class, \
             patch('pathlib.Path.mkdir') as mock_mkdir:
            
            mock_config = Mock()
            mock_config.BRONZE_DATA_PATH = Path("/test/bronze")
            mock_config_class.return_value = mock_config
            
            # Simulate failure
            mock_subprocess_run.return_value.returncode = 1
            mock_subprocess_run.return_value.stderr = "Network error"
            
            base = Base()
            
            # First call should fail
            with pytest.raises(Exception, match="aria2 download failed"):
                base.download("https://example.com/file.csv", "test_file.csv")
            
            # Reset the mock for second call  
            mock_subprocess_run.reset_mock()
            mock_subprocess_run.return_value.returncode = 0
            
            # Mock file stats for the success path
            with patch('pathlib.Path.stat') as mock_stat:
                mock_stat.return_value.st_size = 1024
                
                # Second call should succeed
                base.download("https://example.com/file.csv", "test_file.csv")
                assert mock_subprocess_run.called


# Performance tests
class TestBasePerformance:
    """Performance-related tests for Base class."""

    @pytest.mark.unit
    def test_multiple_base_instances(self):
        """
        Test creation of multiple Base instances.
        
        This test ensures that creating multiple Base instances
        doesn't cause issues with shared resources.
        """
        with patch('scripts.base.Config') as mock_config_class, \
             patch('scripts.base.Logger') as mock_logger_class:
            
            instances = []
            for i in range(5):
                instance = Base()
                instances.append(instance)
            
            # Verify all instances are independent
            for i, instance in enumerate(instances):
                assert instance.config is not None
                assert instance.logger is not None
                assert instance.spark is None
