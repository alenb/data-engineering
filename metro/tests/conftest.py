"""
Pytest configuration and shared fixtures for the metro data pipeline tests.

This module provides common test fixtures and configuration that can be used
across all test modules. Fixtures help set up test data and environments
in a reusable way.
"""

import pytest
import tempfile
import shutil
from pathlib import Path
from unittest.mock import Mock, patch
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from scripts.base import Base
from scripts.bronze import Bronze
from scripts.silver import Silver
from scripts.gold import Gold
from config.config import Config


@pytest.fixture(scope="session")
def spark_session():
    """
    Create a Spark session for testing.
    
    This is a session-scoped fixture, meaning it's created once per test session
    and shared across all tests. This is efficient for Spark which is expensive to start.
    """
    # Stop any existing sessions to ensure clean state
    existing_spark = SparkSession.getActiveSession()
    if existing_spark:
        existing_spark.stop()
    
    builder = (
        SparkSession.builder
        .appName("pytest-spark-testing")
        .master("local[2]")  # Use 2 cores for testing
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0")
        .config("spark.driver.memory", "2g")  # Smaller memory for tests
        .config("spark.executor.memory", "2g")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.adaptive.enabled", "false")  # Disable for predictable testing
        .config("spark.sql.adaptive.coalescePartitions.enabled", "false")
        .config("spark.driver.host", "localhost")  # Explicitly set host for Windows
        .config("spark.ui.enabled", "false")  # Disable Spark UI for tests
    )
    
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    
    # Set log level to reduce noise in tests
    spark.sparkContext.setLogLevel("WARN")
    
    yield spark
    
    # Cleanup after all tests complete
    try:
        spark.stop()
    except Exception:
        # Ignore errors during cleanup
        pass


@pytest.fixture
def temp_dir():
    """
    Create a temporary directory for test files.
    
    This fixture creates a temporary directory that's automatically cleaned up
    after each test. Perfect for testing file operations without cluttering the filesystem.
    """
    temp_path = tempfile.mkdtemp()
    yield Path(temp_path)
    shutil.rmtree(temp_path)


@pytest.fixture
def mock_config(temp_dir):
    """
    Create a mock configuration for testing.
    
    This fixture provides a test configuration that uses temporary directories
    instead of real data paths, preventing tests from affecting production data.
    """
    config = Mock(spec=Config)
    
    # Set up temporary paths
    config.BRONZE_DATA_PATH = str(temp_dir / "bronze")
    config.SILVER_DATA_PATH = str(temp_dir / "silver")
    config.GOLD_DATA_PATH = str(temp_dir / "gold")
    
    # Create the directories
    Path(config.BRONZE_DATA_PATH).mkdir(parents=True, exist_ok=True)
    Path(config.SILVER_DATA_PATH).mkdir(parents=True, exist_ok=True)
    Path(config.GOLD_DATA_PATH).mkdir(parents=True, exist_ok=True)
    
    # Mock API configuration
    config.DATAVIC_BASE_URL = "https://api.data.vic.gov.au/ckan/api/3/action"
    config.DATAVIC_API_KEY = "test-api-key"
    
    return config


@pytest.fixture
def bronze_sample_data(spark_session):
    """
    Load Bronze layer sample data - raw CSV with original schema.
    
    This fixture provides Bronze layer data (21 columns, string time formats)
    for testing Bronze layer functionality and as input for Silver layer tests.
    """
    import os
    
    # Ensure Spark session is active
    try:
        # Test if the session is usable
        spark_session.sparkContext.statusTracker()
    except Exception:
        pytest.skip("Spark session is not active")
    
    # Path to the Bronze sample data CSV file
    sample_data_path = Path(__file__).parent / "fixtures" / "bronze_sample_train_data.csv"
    
    if not sample_data_path.exists():
        pytest.skip(f"Bronze sample data file not found at {sample_data_path}. Run tests/generate_sample_data.py first.")
    
    # Load the Bronze sample data with exact schema
    try:
        df = spark_session.read.csv(
            str(sample_data_path),
            header=True,
            inferSchema=True
        )
        return df
    except Exception as e:
        pytest.skip(f"Failed to load Bronze sample data: {e}")


@pytest.fixture
def sample_train_data(bronze_sample_data):
    """
    Backward compatibility fixture - points to bronze_sample_data.
    
    This maintains compatibility with existing tests while we migrate
    to layer-specific fixtures.
    """
    return bronze_sample_data


@pytest.fixture 
def silver_sample_data_file(spark_session):
    """
    Load Silver layer sample data from the generated Parquet file.
    
    This fixture provides actual Silver layer data (28 columns, timestamps, derived columns)
    directly from the Silver layer processing output.
    """
    import pandas as pd
    
    # Path to the Silver sample data Parquet file
    sample_data_path = Path(__file__).parent / "fixtures" / "silver_sample_train_data.parquet"
    
    if not sample_data_path.exists():
        pytest.skip(f"Silver sample data file not found at {sample_data_path}. Run tests/generate_sample_data.py first.")
    
    # Load the Silver sample data
    pandas_df = pd.read_parquet(sample_data_path)
    df = spark_session.createDataFrame(pandas_df)
    
    return df


@pytest.fixture 
def silver_sample_data(spark_session, bronze_sample_data):
    """
    Create Silver layer sample data by applying Silver transformations to Bronze data.
    
    This fixture takes Bronze data and applies the Silver layer transformations
    to create the expected Silver schema (27 columns, timestamps, derived columns).
    """
    from scripts.silver import Silver
    from unittest.mock import MagicMock
    
    # Create a Silver processor instance
    silver = Silver()
    silver.spark = spark_session
    silver.config = MagicMock()
    silver.logger = MagicMock()
    silver.df = bronze_sample_data
    
    # Apply Silver transformations in order
    silver.clean_negative_passenger_counts()
    silver.fix_arrival_departure_times() 
    silver.extract_time_of_day_info()
    silver.fill_group_with_line_name()
    silver.add_derived_date_parts()
    # Note: Skip fill_missing_passenger_values() as it's complex and not needed for basic testing
    
    return silver.df


@pytest.fixture
def gold_sample_tables(spark_session):
    """
    Load Gold layer sample data from the generated Parquet files.
    
    This fixture provides actual Gold layer dimensional tables and fact table
    directly from the Gold layer processing output.
    """
    import pandas as pd
    
    fixtures_dir = Path(__file__).parent / "fixtures" / "gold"
    
    if not fixtures_dir.exists():
        pytest.skip(f"Gold fixtures directory not found at {fixtures_dir}. Run tests/generate_sample_data.py first.")
    
    gold_tables = {}
    
    # Load all Gold layer tables
    table_files = [
        "dim_date_sample.parquet", "dim_time_bucket_sample.parquet", 
        "dim_train_sample.parquet", "dim_station_sample.parquet",
        "dim_line_sample.parquet", "dim_direction_sample.parquet", 
        "dim_mode_sample.parquet", "fact_train_sample.parquet"
    ]
    
    for table_file in table_files:
        table_path = fixtures_dir / table_file
        if table_path.exists():
            table_name = table_file.replace("_sample.parquet", "")
            pandas_df = pd.read_parquet(table_path)
            gold_tables[table_name] = spark_session.createDataFrame(pandas_df)
    
    return gold_tables


@pytest.fixture
def bronze_processor(mock_config):
    """Create a Bronze processor instance with mocked configuration."""
    with patch('scripts.base.Config', return_value=mock_config):
        processor = Bronze()
        processor.config = mock_config
        return processor


@pytest.fixture
def silver_processor(mock_config, spark_session):
    """Create a Silver processor instance with mocked configuration."""
    with patch('scripts.base.Config', return_value=mock_config):
        processor = Silver()
        processor.config = mock_config
        processor.spark = spark_session
        return processor


@pytest.fixture
def gold_processor(mock_config, spark_session):
    """Create a Gold processor instance with mocked configuration."""
    with patch('scripts.base.Config', return_value=mock_config):
        processor = Gold()
        processor.config = mock_config
        processor.spark = spark_session
        return processor


# Test data files
@pytest.fixture
def create_test_csv(temp_dir, sample_train_data):
    """Create a test CSV file in the temporary directory using real sample data."""
    csv_path = temp_dir / "test_train_patrons.csv"
    # Convert Spark DataFrame to Pandas and save as CSV
    sample_train_data.toPandas().to_csv(csv_path, index=False)
    return str(csv_path)
