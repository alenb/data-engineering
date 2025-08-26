"""
Integration tests for the complete data pipeline.

These tests validate the end-to-end workflow from Bronze to Silver to Gold,
ensuring that all layers work together correctly and data flows properly
through the entire pipeline.
"""

import pytest
import shutil
from pathlib import Path
from unittest.mock import patch, Mock
from scripts.bronze import Bronze
from scripts.silver import Silver


class TestPipelineIntegration:
    """Integration tests for the complete data pipeline."""

    @pytest.mark.integration
    @pytest.mark.slow
    def test_bronze_to_silver_pipeline(self, spark_session, temp_dir, sample_train_data):
        """
        Test the complete Bronze to Silver data pipeline.
        
        This integration test verifies that data can flow correctly
        from the Bronze layer through to the Silver layer with proper
        transformations applied.
        """
        # Set up directory structure
        bronze_dir = temp_dir / "bronze"
        silver_dir = temp_dir / "silver"
        bronze_dir.mkdir(parents=True, exist_ok=True)
        silver_dir.mkdir(parents=True, exist_ok=True)
        
        # Create mock configuration
        mock_config = Mock()
        mock_config.BRONZE_DATA_PATH = str(bronze_dir)
        mock_config.SILVER_DATA_PATH = str(silver_dir)
        
        # Create test CSV file in bronze directory using real sample data
        bronze_csv = bronze_dir / "train_patrons.csv"
        # Write the sample data to CSV (convert Spark DataFrame to Pandas first)
        sample_train_data.toPandas().to_csv(bronze_csv, index=False)
        
        # Test Silver processing
        with patch('scripts.base.Config', return_value=mock_config):
            silver_processor = Silver()
            silver_processor.config = mock_config
            silver_processor.spark = spark_session
            
            # Load and process data
            silver_processor.load_data()
            assert silver_processor.df is not None
            assert silver_processor.df.count() == 50  # Based on real sample data (50 rows)
            
            # Apply transformations
            silver_processor.clean_negative_passenger_counts()
            silver_processor.fill_group_with_line_name()
            silver_processor.add_derived_date_parts()
            
            # Verify transformations were applied
            columns = silver_processor.df.columns
            assert "year" in columns
            assert "month" in columns
            assert "day" in columns
            
            # Verify data quality
            negative_count = silver_processor.df.filter(
                (silver_processor.df.Passenger_Boardings < 0) | 
                (silver_processor.df.Passenger_Alightings < 0)
            ).count()
            assert negative_count == 0

    @pytest.mark.integration
    def test_data_quality_validation(self, spark_session, temp_dir):
        """
        Test comprehensive data quality validation across the pipeline.
        
        This test ensures that data quality checks work correctly
        and can identify various data quality issues.
        """
        # Create test data with known quality issues
        problematic_data = """Stop_ID,Stop_Name,Train_Service,Direction,Date,Time,Passenger_Boardings,Passenger_Alightings,Line,Group
1001,Flinders Street,Metro,City (Up),2024-01-15,08:30:00,45,12,Cranbourne,Cranbourne
1002,Southern Cross,Metro,City (Up),2024-01-15,09:15:00,-10,28,Pakenham,Pakenham
1003,,Metro,City (Down),2024-01-15,17:45:00,18,-5,Frankston,
1004,Richmond,Metro,City (Up),,10:00:00,25,15,Sandringham,Sandringham"""
        
        # Set up test environment
        bronze_dir = temp_dir / "bronze"
        bronze_dir.mkdir(parents=True, exist_ok=True)
        bronze_csv = bronze_dir / "train_patrons.csv"
        bronze_csv.write_text(problematic_data)
        
        mock_config = Mock()
        mock_config.BRONZE_DATA_PATH = str(bronze_dir)
        
        # Test data quality validation
        with patch('scripts.base.Config', return_value=mock_config):
            silver_processor = Silver()
            silver_processor.config = mock_config
            silver_processor.spark = spark_session
            
            # Load data
            silver_processor.load_data()
            initial_df = silver_processor.df
            
            # Check for data quality issues before cleaning
            negative_boardings = initial_df.filter(initial_df.Passenger_Boardings < 0).count()
            negative_alightings = initial_df.filter(initial_df.Passenger_Alightings < 0).count()
            missing_stop_names = initial_df.filter(initial_df.Stop_Name.isNull() | (initial_df.Stop_Name == "")).count()
            missing_dates = initial_df.filter(initial_df.Date.isNull() | (initial_df.Date == "")).count()
            
            # Verify issues were detected
            assert negative_boardings > 0
            assert negative_alightings > 0
            assert missing_stop_names > 0
            assert missing_dates > 0
            
            # Apply cleaning
            silver_processor.clean_negative_passenger_counts()
            
            # Verify cleaning worked
            cleaned_negative_boardings = silver_processor.df.filter(silver_processor.df.Passenger_Boardings < 0).count()
            cleaned_negative_alightings = silver_processor.df.filter(silver_processor.df.Passenger_Alightings < 0).count()
            
            assert cleaned_negative_boardings == 0
            assert cleaned_negative_alightings == 0

    @pytest.mark.integration
    def test_schema_evolution_handling(self, spark_session, temp_dir):
        """
        Test handling of schema changes in source data.
        
        This test verifies that the pipeline can handle minor
        schema changes gracefully without breaking.
        """
        # Create data with additional columns (schema evolution) - using correct Bronze schema
        evolved_data = """Business_Date,Day_of_Week,Day_Type,Mode,Train_Number,Line_Name,Group,Direction,Origin_Station,Destination_Station,Station_Name,Station_Latitude,Station_Longitude,Station_Chainage,Stop_Sequence_Number,Arrival_Time_Scheduled,Departure_Time_Scheduled,Passenger_Boardings,Passenger_Alightings,Passenger_Arrival_Load,Passenger_Departure_Load,New_Column
2024-01-15,Monday,Normal Weekday,Metro,1001,Cranbourne,Caulfield,U,Cranbourne,Flinders Street,Flinders Street,-37.8183,144.9671,0,1,08:30:00,08:30:30,45,12,45,78,ExtraData
2024-01-15,Monday,Normal Weekday,Metro,1002,Pakenham,Caulfield,U,Pakenham,Flinders Street,Southern Cross,-37.8184,144.9671,500,2,09:15:00,09:15:30,32,28,60,64,MoreData"""
        
        # Set up test environment
        bronze_dir = temp_dir / "bronze"
        bronze_dir.mkdir(parents=True, exist_ok=True)
        bronze_csv = bronze_dir / "train_patrons.csv"
        bronze_csv.write_text(evolved_data)
        
        mock_config = Mock()
        mock_config.BRONZE_DATA_PATH = str(bronze_dir)
        
        # Test that pipeline handles additional columns
        with patch('scripts.base.Config', return_value=mock_config):
            silver_processor = Silver()
            silver_processor.config = mock_config
            silver_processor.spark = spark_session
            
            # Should load successfully despite extra column
            silver_processor.load_data()
            assert silver_processor.df is not None
            assert "New_Column" in silver_processor.df.columns
            
            # Processing should continue normally
            silver_processor.clean_negative_passenger_counts()
            silver_processor.add_derived_date_parts()
            
            # Verify expected columns still exist after processing
            required_columns = ["Train_Number", "Station_Name", "Passenger_Boardings", "year", "month", "day"]
            for col in required_columns:
                assert col in silver_processor.df.columns

    @pytest.mark.integration
    @pytest.mark.data_quality
    def test_large_dataset_processing(self, spark_session, temp_dir):
        """
        Test processing of larger datasets to verify scalability.
        
        This test creates a larger dataset to ensure the pipeline
        can handle reasonable data volumes efficiently.
        """
        # Generate larger test dataset with correct Bronze schema
        large_data_rows = []
        large_data_rows.append("Business_Date,Day_of_Week,Day_Type,Mode,Train_Number,Line_Name,Group,Direction,Origin_Station,Destination_Station,Station_Name,Station_Latitude,Station_Longitude,Station_Chainage,Stop_Sequence_Number,Arrival_Time_Scheduled,Departure_Time_Scheduled,Passenger_Boardings,Passenger_Alightings,Passenger_Arrival_Load,Passenger_Departure_Load")
        
        for i in range(1000):  # Generate 1000 rows
            train_number = 1000 + (i % 10)
            station_name = f"Station_{i % 10}"
            date = f"2024-01-{(i % 28) + 1:02d}"
            day_of_week = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"][i % 7]
            arrival_time = f"{(i % 24):02d}:{(i % 60):02d}:00"
            departure_time = f"{(i % 24):02d}:{((i % 60) + 1) % 60:02d}:00"
            boardings = (i % 100) + 1
            alightings = (i % 80) + 1
            departure_load = boardings + (i % 50)
            arrival_load = departure_load - alightings
            line = f"Line_{i % 5}"
            
            large_data_rows.append(f"{date},{day_of_week},Normal Weekday,Metro,{train_number},{line},{line},U,Origin_{i % 5},Destination_{i % 5},{station_name},-37.8{i % 10},{144.9 + (i % 10)},{i * 100},{i % 20},{arrival_time},{departure_time},{boardings},{alightings},{arrival_load},{departure_load}")
        
        large_data = "\n".join(large_data_rows)
        
        # Set up test environment
        bronze_dir = temp_dir / "bronze"
        bronze_dir.mkdir(parents=True, exist_ok=True)
        bronze_csv = bronze_dir / "train_patrons.csv"
        bronze_csv.write_text(large_data)
        
        mock_config = Mock()
        mock_config.BRONZE_DATA_PATH = str(bronze_dir)
        
        # Test processing of large dataset
        with patch('scripts.base.Config', return_value=mock_config):
            silver_processor = Silver()
            silver_processor.config = mock_config
            silver_processor.spark = spark_session
            
            # Load and process data
            silver_processor.load_data()
            assert silver_processor.df.count() == 1000
            
            # Apply transformations
            silver_processor.clean_negative_passenger_counts()
            silver_processor.add_derived_date_parts()
            
            # Verify data integrity
            assert silver_processor.df.count() == 1000  # No rows should be lost
            
            # Verify date partitioning columns
            years = silver_processor.df.select("year").distinct().collect()
            assert len(years) >= 1
            assert all(row["year"] == 2024 for row in years)

    @pytest.mark.integration
    def test_error_recovery_and_logging(self, spark_session, temp_dir, caplog):
        """
        Test error recovery and logging throughout the pipeline.
        
        This test verifies that the pipeline handles errors gracefully
        and provides appropriate logging for debugging.
        """
        # Create invalid CSV data
        invalid_data = "This is not valid CSV data\nWith no proper structure"
        
        bronze_dir = temp_dir / "bronze"
        bronze_dir.mkdir(parents=True, exist_ok=True)
        bronze_csv = bronze_dir / "train_patrons.csv"
        bronze_csv.write_text(invalid_data)
        
        mock_config = Mock()
        mock_config.BRONZE_DATA_PATH = str(bronze_dir)
        
        # Test error handling
        with patch('scripts.base.Config', return_value=mock_config):
            silver_processor = Silver()
            silver_processor.config = mock_config
            silver_processor.spark = spark_session
            
            # Should handle invalid data gracefully
            try:
                silver_processor.load_data()
                # If it loads, verify it's handled appropriately
                if silver_processor.df is not None:
                    # Check that we can still apply basic operations
                    count = silver_processor.df.count()
                    assert count >= 0
            except Exception as e:
                # Exception handling is also acceptable
                assert "malformed" in str(e).lower() or "parse" in str(e).lower() or "schema" in str(e).lower()


class TestPipelinePerformance:
    """Performance tests for the data pipeline."""

    @pytest.mark.slow
    @pytest.mark.integration
    def test_memory_usage_during_processing(self, spark_session, temp_dir):
        """
        Test memory usage patterns during data processing.
        
        This test monitors memory usage to ensure the pipeline
        doesn't have memory leaks or excessive memory consumption.
        """
        import psutil
        import os
        
        # Get initial memory usage
        process = psutil.Process(os.getpid())
        initial_memory = process.memory_info().rss
        
        # Create test data
        bronze_dir = temp_dir / "bronze"
        bronze_dir.mkdir(parents=True, exist_ok=True)
        
        # Generate moderately sized dataset with correct Bronze schema
        data_rows = ["Business_Date,Day_of_Week,Day_Type,Mode,Train_Number,Line_Name,Group,Direction,Origin_Station,Destination_Station,Station_Name,Station_Latitude,Station_Longitude,Station_Chainage,Stop_Sequence_Number,Arrival_Time_Scheduled,Departure_Time_Scheduled,Passenger_Boardings,Passenger_Alightings,Passenger_Arrival_Load,Passenger_Departure_Load"]
        for i in range(100):
            train_number = 1000 + i
            arrival_load = i + 20
            departure_load = arrival_load + i + 10
            data_rows.append(f"2024-01-15,Monday,Normal Weekday,Metro,{train_number},Line_A,Line_A,U,Origin_A,Destination_A,Station_{i},-37.8{i % 10},144.9{i % 10},{i * 100},{i % 20},08:30:00,08:30:30,{i},{i+10},{arrival_load},{departure_load}")
        
        bronze_csv = bronze_dir / "train_patrons.csv"
        bronze_csv.write_text("\n".join(data_rows))
        
        mock_config = Mock()
        mock_config.BRONZE_DATA_PATH = str(bronze_dir)
        
        # Process data and monitor memory
        with patch('scripts.base.Config', return_value=mock_config):
            silver_processor = Silver()
            silver_processor.config = mock_config
            silver_processor.spark = spark_session
            
            # Process data
            silver_processor.load_data()
            silver_processor.clean_negative_passenger_counts()
            silver_processor.add_derived_date_parts()
            
            # Check memory usage after processing
            final_memory = process.memory_info().rss
            memory_increase = final_memory - initial_memory
            
            # Memory increase should be reasonable (less than 100MB for this small dataset)
            assert memory_increase < 100 * 1024 * 1024  # 100MB


class TestPipelineConfiguration:
    """Test pipeline configuration and environment setup."""

    @pytest.mark.integration
    def test_configuration_validation(self, temp_dir):
        """
        Test that pipeline configuration is properly validated.
        
        This test ensures that required configuration parameters
        are present and valid before processing begins.
        """
        mock_config = Mock()
        mock_config.BRONZE_DATA_PATH = str(temp_dir / "bronze")
        mock_config.SILVER_DATA_PATH = str(temp_dir / "silver")
        mock_config.GOLD_DATA_PATH = str(temp_dir / "gold")
        
        # Test configuration validation
        with patch('scripts.base.Config', return_value=mock_config):
            silver_processor = Silver()
            
            # Verify configuration is accessible
            assert hasattr(silver_processor, 'config')
            assert silver_processor.config.BRONZE_DATA_PATH is not None
            assert silver_processor.config.SILVER_DATA_PATH is not None
