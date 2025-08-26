"""
Data quality tests for the metro data pipeline.

These tests focus specifically on data quality validation,
ensuring that data meets business requirements and quality standards
throughout the pipeline processing.
"""

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
from unittest.mock import patch, Mock


class TestDataQuality:
    """Test data quality validation and enforcement."""

    @pytest.mark.data_quality
    def test_passenger_count_ranges(self, spark_session):
        """
        Test that passenger counts are within reasonable ranges.
        
        This test validates that passenger boarding and alighting
        counts are realistic and within expected business limits.
        """
        schema = StructType([
            StructField("Stop_ID", IntegerType(), True),
            StructField("Passenger_Boardings", IntegerType(), True),
            StructField("Passenger_Alightings", IntegerType(), True),
        ])
        
        # Test data with various passenger count scenarios
        test_data = [
            (1001, 50, 30),      # Normal counts
            (1002, 0, 0),        # Zero counts (valid)
            (1003, 1000, 800),   # High but reasonable counts
            (1004, -10, 20),     # Invalid negative boarding
            (1005, 25, -5),      # Invalid negative alighting
            (1006, 10000, 6000), # Unrealistically high counts
        ]
        
        df = spark_session.createDataFrame(test_data, schema)
        
        # Test for negative values
        negative_boardings = df.filter(F.col("Passenger_Boardings") < 0).count()
        negative_alightings = df.filter(F.col("Passenger_Alightings") < 0).count()
        
        assert negative_boardings == 1  # One row has negative boardings
        assert negative_alightings == 1  # One row has negative alightings
        
        # Test for unrealistic values (business rule: >5000 passengers unusual for single trip)
        high_boardings = df.filter(F.col("Passenger_Boardings") > 5000).count()
        high_alightings = df.filter(F.col("Passenger_Alightings") > 5000).count()
        
        assert high_boardings == 1  # One row has unrealistically high boardings
        assert high_alightings == 1  # One row has unrealistically high alightings
        
        # Test for valid range (0-5000)
        valid_boardings = df.filter(
            (F.col("Passenger_Boardings") >= 0) & 
            (F.col("Passenger_Boardings") <= 5000)
        ).count()
        
        assert valid_boardings == 4  # Four rows have valid boarding counts

    @pytest.mark.data_quality
    def test_required_fields_completeness(self, spark_session):
        """
        Test completeness of required fields.
        
        This test validates that critical fields required for
        downstream processing are not null or empty.
        """
        schema = StructType([
            StructField("Stop_ID", IntegerType(), True),
            StructField("Stop_Name", StringType(), True),
            StructField("Date", StringType(), True),
            StructField("Time", StringType(), True),
            StructField("Line", StringType(), True),
        ])
        
        test_data = [
            (1001, "Flinders Street", "2024-01-15", "08:30:00", "Cranbourne"),  # Complete
            (1002, "", "2024-01-15", "09:15:00", "Pakenham"),                  # Missing stop name
            (1003, "Melbourne Central", "", "17:45:00", "Frankston"),          # Missing date
            (1004, "Richmond", "2024-01-15", "", "Sandringham"),               # Missing time
            (None, "South Yarra", "2024-01-15", "14:20:00", "Glen Waverley"),  # Missing stop ID
            (1006, "Caulfield", "2024-01-15", "11:30:00", ""),                 # Missing line
        ]
        
        df = spark_session.createDataFrame(test_data, schema)
        
        # Test completeness of each required field
        total_rows = df.count()
        
        # Stop_ID completeness
        complete_stop_id = df.filter(F.col("Stop_ID").isNotNull()).count()
        stop_id_completeness = complete_stop_id / total_rows
        assert stop_id_completeness == 5/6  # 5 out of 6 rows have Stop_ID
        
        # Stop_Name completeness (non-empty strings)
        complete_stop_name = df.filter(
            (F.col("Stop_Name").isNotNull()) & 
            (F.col("Stop_Name") != "")
        ).count()
        stop_name_completeness = complete_stop_name / total_rows
        assert stop_name_completeness == 5/6  # 5 out of 6 rows have non-empty Stop_Name
        
        # Date completeness
        complete_date = df.filter(
            (F.col("Date").isNotNull()) & 
            (F.col("Date") != "")
        ).count()
        date_completeness = complete_date / total_rows
        assert date_completeness == 5/6  # 5 out of 6 rows have Date
        
        # Overall record completeness (all required fields present)
        complete_records = df.filter(
            (F.col("Stop_ID").isNotNull()) &
            (F.col("Stop_Name").isNotNull()) & (F.col("Stop_Name") != "") &
            (F.col("Date").isNotNull()) & (F.col("Date") != "") &
            (F.col("Time").isNotNull()) & (F.col("Time") != "") &
            (F.col("Line").isNotNull()) & (F.col("Line") != "")
        ).count()
        
        overall_completeness = complete_records / total_rows
        assert overall_completeness == 1/6  # Only 1 out of 6 rows is completely valid

    @pytest.mark.data_quality
    def test_date_format_validation(self, spark_session):
        """
        Test validation of date formats.
        
        This test ensures that date fields conform to expected
        formats and can be properly parsed.
        """
        schema = StructType([
            StructField("Stop_ID", IntegerType(), True),
            StructField("Date", StringType(), True),
            StructField("Time", StringType(), True),
        ])
        
        test_data = [
            (1001, "2024-01-15", "08:30:00"),      # Valid format
            (1002, "2024/01/15", "09:15:00"),      # Different date separator
            (1003, "15-01-2024", "17:45:00"),      # Different date order
            (1004, "2024-13-45", "10:00:00"),      # Invalid month/day
            (1005, "invalid-date", "14:20:00"),    # Completely invalid date
            (1006, "2024-01-15", "25:30:00"),      # Invalid time (hour > 24)
            (1007, "2024-01-15", "08:75:00"),      # Invalid time (minute > 60)
        ]
        
        df = spark_session.createDataFrame(test_data, schema)
        
        # Test date parsing - valid dates should parse successfully
        df_with_parsed_date = df.withColumn(
            "parsed_date", 
            F.to_date(F.col("Date"), "yyyy-MM-dd")
        )
        
        valid_dates = df_with_parsed_date.filter(
            F.col("parsed_date").isNotNull()
        ).count()
        
        assert valid_dates == 3  # Three rows have valid yyyy-MM-dd date format
        
        # Test time parsing
        df_with_parsed_time = df.withColumn(
            "parsed_time",
            F.to_timestamp(F.concat(F.lit("2024-01-01 "), F.col("Time")), "yyyy-MM-dd HH:mm:ss")
        )
        
        valid_times = df_with_parsed_time.filter(
            F.col("parsed_time").isNotNull()
        ).count()
        
        # Valid times should be those with proper HH:mm:ss format and valid values
        assert valid_times >= 4  # At least the first 4 rows should have parseable times

    @pytest.mark.data_quality
    def test_referential_integrity(self, spark_session):
        """
        Test referential integrity between related fields.
        
        This test validates that related fields have consistent
        values and maintain logical relationships.
        """
        schema = StructType([
            StructField("Stop_ID", IntegerType(), True),
            StructField("Stop_Name", StringType(), True),
            StructField("Line", StringType(), True),
            StructField("Group", StringType(), True),
        ])
        
        test_data = [
            (1001, "Flinders Street", "Cranbourne", "Cranbourne"),    # Consistent
            (1002, "Southern Cross", "Pakenham", "Pakenham"),        # Consistent
            (1003, "Melbourne Central", "Frankston", "Dandenong"),    # Inconsistent Line/Group
            (1001, "Richmond", "Cranbourne", "Cranbourne"),          # Same Stop_ID, different name
            (1005, "Flinders Street", "Glen Waverley", "Glen Waverley"), # Same name, different Stop_ID
        ]
        
        df = spark_session.createDataFrame(test_data, schema)
        
        # Test Stop_ID to Stop_Name consistency
        stop_consistency = df.groupBy("Stop_ID").agg(
            F.countDistinct("Stop_Name").alias("name_count")
        )
        
        inconsistent_stops = stop_consistency.filter(F.col("name_count") > 1).count()
        assert inconsistent_stops == 1  # Stop_ID 1001 has two different names
        
        # Test Stop_Name to Stop_ID consistency
        name_consistency = df.groupBy("Stop_Name").agg(
            F.countDistinct("Stop_ID").alias("id_count")
        )
        
        inconsistent_names = name_consistency.filter(F.col("id_count") > 1).count()
        assert inconsistent_names == 1  # "Flinders Street" has two different IDs
        
        # Test Line to Group consistency (should be same in most cases)
        line_group_matches = df.filter(F.col("Line") == F.col("Group")).count()
        total_rows = df.count()
        consistency_rate = line_group_matches / total_rows
        
        assert consistency_rate == 4/5  # 4 out of 5 rows have matching Line and Group

    @pytest.mark.data_quality
    def test_business_rule_validation(self, spark_session):
        """
        Test validation of business rules.
        
        This test validates domain-specific business rules
        that the data should satisfy.
        """
        schema = StructType([
            StructField("Stop_ID", IntegerType(), True),
            StructField("Direction", StringType(), True),
            StructField("Passenger_Boardings", IntegerType(), True),
            StructField("Passenger_Alightings", IntegerType(), True),
            StructField("Time", StringType(), True),
            StructField("Date", StringType(), True),
        ])
        
        test_data = [
            (1001, "City (Up)", 45, 12, "08:30:00", "2024-01-15"),    # Normal weekday morning
            (1002, "City (Down)", 15, 50, "17:45:00", "2024-01-15"),  # Normal weekday evening
            (1003, "City (Up)", 100, 5, "02:30:00", "2024-01-15"),    # Unusual: high boarding at 2:30 AM
            (1004, "City (Down)", 5, 80, "09:00:00", "2024-01-13"),   # Weekend pattern
            (1005, "Invalid Direction", 20, 20, "12:00:00", "2024-01-15"), # Invalid direction
        ]
        
        df = spark_session.createDataFrame(test_data, schema)
        
        # Business Rule 1: Valid direction values
        valid_directions = ["City (Up)", "City (Down)", "City Bound", "Outbound"]
        valid_direction_count = df.filter(F.col("Direction").isin(valid_directions)).count()
        assert valid_direction_count == 4  # 4 out of 5 rows have valid directions
        
        # Business Rule 2: During morning peak (7-9 AM), more boardings toward city
        morning_peak = df.filter(
            (F.col("Time") >= "07:00:00") & 
            (F.col("Time") <= "09:00:00") &
            (F.col("Direction") == "City (Up)")
        )
        
        # Should generally have more boardings than alightings in morning peak toward city
        morning_peak_with_ratio = morning_peak.withColumn(
            "more_boardings", 
            F.col("Passenger_Boardings") > F.col("Passenger_Alightings")
        )
        
        morning_boardings_higher = morning_peak_with_ratio.filter(
            F.col("more_boardings") == True
        ).count()
        
        # This business rule should hold for most morning peak City (Up) trips
        assert morning_boardings_higher >= 1
        
        # Business Rule 3: Evening peak (5-7 PM), more alightings from city
        evening_peak = df.filter(
            (F.col("Time") >= "17:00:00") & 
            (F.col("Time") <= "19:00:00") &
            (F.col("Direction") == "City (Down)")
        )
        
        evening_peak_with_ratio = evening_peak.withColumn(
            "more_alightings",
            F.col("Passenger_Alightings") > F.col("Passenger_Boardings")
        )
        
        evening_alightings_higher = evening_peak_with_ratio.filter(
            F.col("more_alightings") == True
        ).count()
        
        assert evening_alightings_higher >= 1

    @pytest.mark.data_quality
    def test_statistical_outliers(self, spark_session):
        """
        Test detection of statistical outliers in passenger data.
        
        This test identifies data points that are statistical outliers
        and may indicate data quality issues.
        """
        schema = StructType([
            StructField("Stop_ID", IntegerType(), True),
            StructField("Passenger_Boardings", IntegerType(), True),
        ])
        
        # Create data with known outliers
        normal_data = [(1000 + i, 25 + (i % 10)) for i in range(20)]  # Normal range: 25-34
        outlier_data = [(1020, 500), (1021, 1000)]  # Clear outliers
        
        test_data = normal_data + outlier_data
        df = spark_session.createDataFrame(test_data, schema)
        
        # Calculate statistics for outlier detection
        stats = df.select(
            F.mean("Passenger_Boardings").alias("mean"),
            F.stddev("Passenger_Boardings").alias("stddev")
        ).collect()[0]
        
        mean_boardings = stats["mean"]
        stddev_boardings = stats["stddev"]
        
        # Define outliers as values > 3 standard deviations from mean
        df_with_outliers = df.withColumn(
            "is_outlier",
            F.abs(F.col("Passenger_Boardings") - mean_boardings) > (3 * stddev_boardings)
        )
        
        outlier_count = df_with_outliers.filter(F.col("is_outlier") == True).count()
        
        # Should detect at least 1 outlier (the 1000 value is definitely an outlier)
        assert outlier_count >= 1

    @pytest.mark.data_quality
    def test_temporal_consistency(self, spark_session):
        """
        Test temporal consistency in the data.
        
        This test validates that time-related fields are consistent
        and follow logical temporal patterns.
        """
        from datetime import datetime, timedelta
        
        schema = StructType([
            StructField("Stop_ID", IntegerType(), True),
            StructField("Date", StringType(), True),
            StructField("Time", StringType(), True),
            StructField("Stop_Sequence_Number", IntegerType(), True),
        ])
        
        # Create time series data for the same train journey
        base_time = datetime(2024, 1, 15, 8, 0, 0)
        test_data = []
        
        for i in range(5):
            current_time = base_time + timedelta(minutes=i*3)  # 3 minutes between stops
            test_data.append((
                1000 + i,
                "2024-01-15",
                current_time.strftime("%H:%M:%S"),
                i + 1
            ))
        
        # Add an inconsistent time (earlier than previous stop)
        inconsistent_time = base_time + timedelta(minutes=1)  # Goes back in time
        test_data.append((
            1005,
            "2024-01-15", 
            inconsistent_time.strftime("%H:%M:%S"),
            6
        ))
        
        df = spark_session.createDataFrame(test_data, schema)
        
        # Convert time strings to timestamps for comparison
        df_with_timestamps = df.withColumn(
            "timestamp",
            F.to_timestamp(F.concat(F.col("Date"), F.lit(" "), F.col("Time")), "yyyy-MM-dd HH:mm:ss")
        )
        
        # Check temporal ordering within the sequence
        from pyspark.sql.window import Window
        window_spec = Window.orderBy("Stop_Sequence_Number")
        
        df_with_prev_time = df_with_timestamps.withColumn(
            "prev_timestamp",
            F.lag("timestamp").over(window_spec)
        )
        
        # Count cases where time goes backwards
        time_inconsistencies = df_with_prev_time.filter(
            (F.col("prev_timestamp").isNotNull()) &
            (F.col("timestamp") < F.col("prev_timestamp"))
        ).count()
        
        assert time_inconsistencies == 1  # Should detect the one inconsistent time we added


class TestDataQualityMetrics:
    """Test calculation of data quality metrics."""

    @pytest.mark.data_quality
    def test_completeness_metrics(self, spark_session, sample_train_data):
        """
        Test calculation of data completeness metrics.
        
        This test validates that completeness metrics are
        correctly calculated for data quality reporting.
        """
        # Calculate completeness for each column
        total_rows = sample_train_data.count()
        
        completeness_metrics = {}
        for column in sample_train_data.columns:
            non_null_count = sample_train_data.filter(F.col(column).isNotNull()).count()
            completeness_metrics[column] = non_null_count / total_rows
        
        # Verify completeness calculations
        assert all(0 <= completeness <= 1 for completeness in completeness_metrics.values())
        
        # Test specific completeness expectations based on sample data
        assert completeness_metrics["Station_Name"] == 1.0  # Should be 100% complete
        assert completeness_metrics["Line_Name"] == 1.0  # Should be 100% complete
        
        # All passenger counts should be complete in the real sample data
        assert completeness_metrics["Passenger_Boardings"] == 1.0

    @pytest.mark.data_quality
    def test_validity_metrics(self, spark_session):
        """
        Test calculation of data validity metrics.
        
        This test validates that validity metrics correctly
        identify valid vs invalid data values.
        """
        schema = StructType([
            StructField("Stop_ID", IntegerType(), True),
            StructField("Passenger_Boardings", IntegerType(), True),
            StructField("Date", StringType(), True),
        ])
        
        test_data = [
            (1001, 45, "2024-01-15"),     # All valid
            (1002, -10, "2024-01-15"),    # Invalid passenger count
            (1003, 30, "invalid-date"),   # Invalid date
            (1004, -5, "invalid-date"),   # Multiple invalid fields
        ]
        
        df = spark_session.createDataFrame(test_data, schema)
        total_rows = df.count()
        
        # Calculate validity metrics
        valid_passenger_count = df.filter(F.col("Passenger_Boardings") >= 0).count()
        passenger_validity = valid_passenger_count / total_rows
        
        valid_date_count = df.filter(
            F.to_date(F.col("Date"), "yyyy-MM-dd").isNotNull()
        ).count()
        date_validity = valid_date_count / total_rows
        
        # Verify validity calculations
        assert passenger_validity == 0.5  # 2 out of 4 have valid passenger counts
        assert date_validity == 0.5       # 2 out of 4 have valid dates
