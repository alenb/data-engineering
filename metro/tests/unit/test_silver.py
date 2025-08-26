"""
Unit tests for the Silver layer data processing.

These tests validate individual methods of the Silver class to ensure
data transformations work correctly. Each test focuses on a single
method and its expected behaviour.
"""

import pytest
from unittest.mock import patch, Mock
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from scripts.silver import Silver


class TestSilver:
    """Test class for Silver layer functionality."""

    @pytest.mark.unit
    def test_init(self, mock_config):
        """Test Silver class initialization."""
        with patch('scripts.base.Config', return_value=mock_config):
            silver = Silver()
            assert silver.config == mock_config
            assert silver.df is None
            assert silver.spark is None

    @pytest.mark.unit
    def test_clean_negative_passenger_counts(self, silver_processor, sample_train_data):
        """
        Test that negative passenger counts are replaced with null values.
        
        This is a critical data quality test - negative passenger counts
        are nonsensical and should be cleaned.
        """
        # Set up the test data
        silver_processor.df = sample_train_data
        
        # Get initial counts
        initial_count = sample_train_data.count()
        negative_boardings = sample_train_data.filter(F.col("Passenger_Boardings") < 0).count()
        negative_alightings = sample_train_data.filter(F.col("Passenger_Alightings") < 0).count()
        
        # Apply the cleaning method
        silver_processor.clean_negative_passenger_counts()
        
        # Verify no rows were dropped
        assert silver_processor.df.count() == initial_count
        
        # Verify negative values are now null
        null_boardings = silver_processor.df.filter(F.col("Passenger_Boardings").isNull()).count()
        null_alightings = silver_processor.df.filter(F.col("Passenger_Alightings").isNull()).count()
        
        # The number of nulls should include original nulls plus converted negatives
        assert null_boardings >= negative_boardings
        assert null_alightings >= negative_alightings
        
        # Verify no negative values remain
        remaining_negative_boardings = silver_processor.df.filter(F.col("Passenger_Boardings") < 0).count()
        remaining_negative_alightings = silver_processor.df.filter(F.col("Passenger_Alightings") < 0).count()
        
        assert remaining_negative_boardings == 0
        assert remaining_negative_alightings == 0

    @pytest.mark.unit
    def test_extract_time_of_day_info(self, silver_processor, bronze_sample_data):
        """
        Test time-of-day information extraction and time bucket creation.
        
        Tests Bronze → Silver transformation: string times → timestamps + time analysis columns.
        Input: Bronze schema (string time columns)
        Output: Silver schema (timestamp columns + derived time columns)
        """
        from pyspark.sql.types import TimestampType
        
        # Use Bronze data as input (string time format)
        silver_processor.df = bronze_sample_data
        
        # First apply timestamp conversion (prerequisite for time extraction)
        silver_processor.fix_arrival_departure_times()
        
        # Now apply the time extraction
        silver_processor.extract_time_of_day_info()
        
        # Collect results for verification
        results = silver_processor.df.collect()
        
        # Verify new columns exist (Silver layer additions)
        expected_columns = [
            "Arrival_Time", "Departure_Time", 
            "Arrival_Time_Bucket", "Departure_Time_Bucket"
        ]
        for col in expected_columns:
            assert col in silver_processor.df.columns
        
        # Verify timestamp columns are now TimestampType (Bronze → Silver transformation)
        schema_dict = {field.name: field.dataType for field in silver_processor.df.schema.fields}
        assert isinstance(schema_dict["Arrival_Time_Scheduled"], TimestampType)
        assert isinstance(schema_dict["Departure_Time_Scheduled"], TimestampType)
        
        # Verify time bucket values are reasonable (0-1440 minutes in day, 30-min increments)
        for row in results:
            if row["Arrival_Time_Bucket"] is not None:
                assert 0 <= row["Arrival_Time_Bucket"] <= 1440
                assert row["Arrival_Time_Bucket"] % 30 == 0  # Must be 30-minute increments
            if row["Departure_Time_Bucket"] is not None:
                assert 0 <= row["Departure_Time_Bucket"] <= 1440
                assert row["Departure_Time_Bucket"] % 30 == 0

    @pytest.mark.unit
    def test_fill_group_with_line_name(self, silver_processor, bronze_sample_data):
        """
        Test filling missing Group values with Line_Name.
        
        Tests Silver layer data completion logic using real Bronze data.
        This verifies the business rule: when Group is NULL, fill with Line_Name.
        """
        # Use Bronze data as input 
        silver_processor.df = bronze_sample_data
        
        # Create some test data with missing Group values to verify the logic
        from pyspark.sql import functions as F
        
        # Add some NULL Group values to test the filling logic
        test_df = silver_processor.df.withColumn(
            "Group", 
            F.when(F.col("Stop_Sequence_Number") % 3 == 0, None)  # Make every 3rd row have NULL Group
            .otherwise(F.col("Group"))
        )
        silver_processor.df = test_df
        
        # Apply the method
        silver_processor.fill_group_with_line_name()
        
        results = silver_processor.df.collect()
        
        # Verify that when Group was NULL, it got filled with Line_Name
        for row in results:
            if row["Line_Name"] is not None:
                # If Line_Name exists, Group should not be NULL (should be filled)
                assert row["Group"] is not None, f"Group should be filled when Line_Name='{row['Line_Name']}' exists"
                
        # Check that at least some filling occurred by comparing before/after
        original_nulls = bronze_sample_data.filter(F.col("Group").isNull()).count()
        final_nulls = silver_processor.df.filter(F.col("Group").isNull()).count()
        
        # After filling, there should be fewer or equal NULL Group values
        assert final_nulls <= original_nulls, "Group filling should reduce or maintain NULL count"

    @pytest.mark.unit
    def test_add_derived_date_parts(self, silver_processor, bronze_sample_data):
        """
        Test addition of year, month, day columns for partitioning.
        
        Tests Silver layer date partitioning using real Bronze data.
        Input: Bronze Business_Date column
        Output: Silver year, month, day columns for partitioning
        """
        
        # Use Bronze data as input
        silver_processor.df = bronze_sample_data
        
        # Apply the method
        silver_processor.add_derived_date_parts()
        
        results = silver_processor.df.collect()
        
        # Verify new Silver columns exist
        for col in ["year", "month", "day"]:
            assert col in silver_processor.df.columns
        
        # Verify that date extraction worked correctly using real data
        for row in results:
            business_date = row["Business_Date"]
            if business_date is not None:
                # Extract expected values from the Business_Date string (Bronze format)
                if isinstance(business_date, str):
                    # Parse string date format from Bronze layer
                    from datetime import datetime
                    parsed_date = datetime.strptime(business_date, "%Y-%m-%d")
                    expected_year = parsed_date.year
                    expected_month = parsed_date.month  
                    expected_day = parsed_date.day
                else:
                    # If already a date object
                    expected_year = business_date.year
                    expected_month = business_date.month
                    expected_day = business_date.day
                
                # Verify extracted date parts match
                assert row["year"] == expected_year, f"Year mismatch for {business_date}"
                assert row["month"] == expected_month, f"Month mismatch for {business_date}"
                assert row["day"] == expected_day, f"Day mismatch for {business_date}"

    @pytest.mark.unit
    @pytest.mark.spark
    def test_load_data_success(self, silver_processor, create_test_csv, mock_config):
        """
        Test successful data loading from CSV file.
        
        This test verifies that the Silver processor can correctly
        load data from the Bronze layer CSV file.
        """
        # Update config to point to our test CSV
        mock_config.BRONZE_DATA_PATH = str(create_test_csv).replace("test_train_patrons.csv", "").rstrip("/")
        
        # Create the expected CSV file in the bronze path
        import shutil
        bronze_csv_path = f"{mock_config.BRONZE_DATA_PATH}/train_patrons.csv"
        shutil.copy(create_test_csv, bronze_csv_path)
        
        # Apply the method
        silver_processor.load_data()
        
        # Verify data was loaded
        assert silver_processor.df is not None
        assert silver_processor.df.count() > 0
        
        # Verify expected columns exist (using actual column names from real data)
        expected_columns = [
            "Business_Date", "Station_Name", "Line_Name", "Direction", 
            "Passenger_Boardings", "Passenger_Alightings", "Passenger_Arrival_Load", "Passenger_Departure_Load"
        ]
        for col in expected_columns:
            assert col in silver_processor.df.columns

    @pytest.mark.unit
    def test_load_data_file_not_found(self, silver_processor, mock_config):
        """
        Test data loading when CSV file doesn't exist.
        
        This test ensures appropriate error handling when the
        source data file is missing.
        """
        # Set a path that doesn't exist
        mock_config.BRONZE_DATA_PATH = "/nonexistent/path"
        
        # This should raise an exception
        with pytest.raises(Exception):
            silver_processor.load_data()

    @pytest.mark.integration
    @pytest.mark.slow
    def test_silver_processing_pipeline(self, silver_processor, create_test_csv, mock_config):
        """
        Integration test for the complete Silver processing pipeline.
        
        This test runs the entire Silver processing workflow to ensure
        all methods work together correctly.
        """
        # Set up the test environment
        mock_config.BRONZE_DATA_PATH = str(create_test_csv).replace("test_train_patrons.csv", "").rstrip("/")
        
        # Create the expected CSV file in the bronze path
        import shutil
        bronze_csv_path = f"{mock_config.BRONZE_DATA_PATH}/train_patrons.csv"
        shutil.copy(create_test_csv, bronze_csv_path)
        
        # Mock the process method to avoid actual file writing in tests
        with patch.object(silver_processor, 'process') as mock_process:
            # Run the pipeline (excluding the create_spark call which is mocked)
            silver_processor.load_data()
            silver_processor.clean_negative_passenger_counts()
            silver_processor.fill_group_with_line_name()
            silver_processor.add_derived_date_parts()
            silver_processor.fill_missing_passenger_values()
            
            # Verify the DataFrame exists and has expected structure
            assert silver_processor.df is not None
            assert silver_processor.df.count() > 0
            
            # Verify data quality improvements
            negative_count = silver_processor.df.filter(
                (F.col("Passenger_Boardings") < 0) | (F.col("Passenger_Alightings") < 0)
            ).count()
            assert negative_count == 0
            
            # Verify partitioning columns exist
            for col in ["year", "month", "day"]:
                assert col in silver_processor.df.columns


# Parameterised tests for different data scenarios
class TestSilverDataScenarios:
    """Test various data scenarios and edge cases."""

    @pytest.mark.parametrise("boarding_value,expected_result", [
        (50, 50),      # Positive value unchanged
        (-10, None),   # Negative becomes null
        (0, 0),        # Zero unchanged
        (None, None),  # Null unchanged
    ])
    def test_passenger_count_cleaning_scenarios(self, silver_processor, spark_session, boarding_value, expected_result):
        """
        Parameterised test for different passenger count scenarios.
        
        This test uses pytest's parametrise decorator to test multiple
        scenarios with a single test function.
        """
        from pyspark.sql.types import StructType, StructField, StringType, IntegerType
        
        # Ensure Spark session is active
        if spark_session._jsparkSession is None or spark_session.sparkContext._jsc is None:
            pytest.skip("Spark session not available")
        
        # Create a minimal schema that includes all required columns for the cleaning method
        schema = StructType([
            StructField("Business_Date", StringType(), True),
            StructField("Station_Name", StringType(), True),
            StructField("Passenger_Boardings", IntegerType(), True),
            StructField("Passenger_Alightings", IntegerType(), True),
            StructField("Passenger_Arrival_Load", IntegerType(), True),
            StructField("Passenger_Departure_Load", IntegerType(), True),
        ])
        
        test_data = [("2023-07-01", "Test Station", boarding_value, 20, 100, 120)]
        silver_processor.df = spark_session.createDataFrame(test_data, schema)
        
        silver_processor.clean_negative_passenger_counts()
        
        result = silver_processor.df.collect()[0]["Passenger_Boardings"]
        assert result == expected_result


# Performance and data quality tests
class TestSilverDataQuality:
    """Test data quality and performance aspects."""

    @pytest.mark.data_quality
    def test_no_duplicate_rows_created(self, silver_processor, sample_train_data):
        """
        Test that processing doesn't create duplicate rows.
        
        Data quality test to ensure transformations don't
        accidentally duplicate records.
        """
        initial_count = sample_train_data.count()
        silver_processor.df = sample_train_data
        
        # Apply several transformations
        silver_processor.clean_negative_passenger_counts()
        silver_processor.fill_group_with_line_name()
        silver_processor.add_derived_date_parts()
        
        final_count = silver_processor.df.count()
        assert final_count == initial_count

    @pytest.mark.data_quality
    def test_required_columns_preserved(self, silver_processor, sample_train_data):
        """
        Test that essential columns are preserved through processing.

        Ensures that critical columns for downstream processing
        are not accidentally dropped.
        """
        # Use the actual column names from the real data
        critical_columns = ["Business_Date", "Station_Name", "Line_Name", "Passenger_Boardings", "Passenger_Alightings"]

        silver_processor.df = sample_train_data
        silver_processor.clean_negative_passenger_counts()
        silver_processor.fill_group_with_line_name()

        for col in critical_columns:
            assert col in silver_processor.df.columns