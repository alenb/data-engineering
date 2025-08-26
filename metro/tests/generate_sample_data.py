"""
Generate sample test data by randomly sampling from each pipeline layer.

This creates layer-specific fixtures:
- Bronze: Raw CSV sample (for Bronze layer tests)
- Silver: Transformed Delta sample (for Silver layer tests) 
- Gold: Star schema samples (for Gold layer tests)

This ensures our test data has the correct schema and realistic values for each layer.
"""

import pandas as pd
import random
from pathlib import Path
import sys
import os

# Add the project root to the path so we can import scripts
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def generate_bronze_sample():
    """Generate sample test data from the Bronze layer CSV."""
    
    # Path to the Bronze data
    bronze_data_path = Path("data/bronze/train_patrons.csv")
    sample_data_path = Path("tests/fixtures/bronze_sample_train_data.csv")
    
    # Ensure the fixtures directory exists
    sample_data_path.parent.mkdir(parents=True, exist_ok=True)
    
    print(f"Reading Bronze data from: {bronze_data_path}")
    
    # Read the Bronze CSV file
    df = pd.read_csv(bronze_data_path)
    
    print(f"Bronze data shape: {df.shape}")
    print(f"Bronze columns: {list(df.columns)}")
    
    # Set random seed for reproducible test data
    random.seed(42)
    
    # Sample 50 random rows to get good diversity
    sample_size = min(50, len(df))
    sampled_df = df.sample(n=sample_size, random_state=42)
    
    # Sort by Business_Date and Arrival_Time_Scheduled for easier debugging
    sampled_df = sampled_df.sort_values(['Business_Date', 'Arrival_Time_Scheduled'])
    
    # Save to the fixtures directory
    sampled_df.to_csv(sample_data_path, index=False)
    
    print(f"Generated Bronze sample with {len(sampled_df)} rows")
    print(f"Bronze sample saved to: {sample_data_path}")
    
    return sampled_df

def generate_silver_sample():
    """Generate sample test data from the Silver layer Delta format."""
    try:
        from pyspark.sql import SparkSession
        from delta import configure_spark_with_delta_pip
        
        # Create Spark session using same config as Base class
        builder = (
            SparkSession.builder
            .appName("GenerateSilverSample")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0")
            .config("spark.driver.memory", "4g")
            .config("spark.executor.memory", "4g")
            .config("spark.memory.fraction", "0.6")
            .config("spark.memory.storageFraction", "0.5")
        )
        spark = configure_spark_with_delta_pip(builder).getOrCreate()
        
        # Path to Silver data
        silver_data_path = "data/silver/train_patrons"
        sample_data_path = Path("tests/fixtures/silver_sample_train_data.parquet")
        
        print(f"Reading Silver data from: {silver_data_path}")
        
        # Read Silver Delta table
        silver_df = spark.read.format("delta").load(silver_data_path)
        
        print(f"Silver data columns: {silver_df.columns}")
        print(f"Silver data count: {silver_df.count()}")
        
        # Sample data and convert to Pandas for easier handling in tests
        sampled_df = silver_df.sample(fraction=0.001, seed=42).limit(50)
        pandas_df = sampled_df.toPandas()
        
        # Sort by Business_Date 
        pandas_df = pandas_df.sort_values(['Business_Date'])
        
        # Save as Parquet (maintains schema better than CSV for complex types)
        pandas_df.to_parquet(sample_data_path, index=False)
        
        print(f"Generated Silver sample with {len(pandas_df)} rows")
        print(f"Silver sample saved to: {sample_data_path}")
        
        # Also save schema info
        schema_path = Path("tests/fixtures/silver_schema_info.txt")
        with open(schema_path, 'w') as f:
            f.write("Silver Layer Schema:\n")
            f.write("===================\n")
            for field in sampled_df.schema.fields:
                f.write(f"{field.name}: {field.dataType}\n")
        
        spark.stop()
        return pandas_df
        
    except Exception as e:
        print(f"Could not generate Silver sample: {e}")
        print("This is likely because Silver layer hasn't been run yet or Spark/Delta dependencies are missing")
        return None

def generate_gold_samples():
    """Generate sample test data from the Gold layer dimension and fact tables."""
    try:
        from pyspark.sql import SparkSession
        from delta import configure_spark_with_delta_pip
        
        # Create Spark session using same config as Base class
        builder = (
            SparkSession.builder
            .appName("GenerateGoldSamples")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0")
            .config("spark.driver.memory", "4g")
            .config("spark.executor.memory", "4g")
            .config("spark.memory.fraction", "0.6")
            .config("spark.memory.storageFraction", "0.5")
        )
        spark = configure_spark_with_delta_pip(builder).getOrCreate()
        
        gold_tables = [
            "dim_date", "dim_time_bucket", "dim_train", "dim_station", 
            "dim_line", "dim_direction", "dim_mode", "fact_train"
        ]
        
        fixtures_dir = Path("tests/fixtures/gold")
        fixtures_dir.mkdir(parents=True, exist_ok=True)
        
        for table_name in gold_tables:
            try:
                table_path = f"data/gold/{table_name}"
                sample_path = fixtures_dir / f"{table_name}_sample.parquet"
                
                print(f"Reading Gold table: {table_name}")
                
                # Read Gold Delta table
                table_df = spark.read.format("delta").load(table_path)
                
                # For dimension tables, take all data (usually small)
                # For fact table, sample it
                if table_name == "fact_train":
                    sampled_df = table_df.sample(fraction=0.001, seed=42).limit(100)
                else:
                    sampled_df = table_df.limit(50)  # Just take first 50 for dims
                
                pandas_df = sampled_df.toPandas()
                pandas_df.to_parquet(sample_path, index=False)
                
                print(f"Generated {table_name} sample with {len(pandas_df)} rows")
                
            except Exception as e:
                print(f"Could not generate sample for {table_name}: {e}")
        
        # Generate schema documentation
        schema_path = fixtures_dir / "gold_schemas_info.txt"
        with open(schema_path, 'w') as f:
            f.write("Gold Layer Schemas:\n")
            f.write("==================\n\n")
            
            for table_name in gold_tables:
                try:
                    table_path = f"data/gold/{table_name}"
                    table_df = spark.read.format("delta").load(table_path)
                    
                    f.write(f"{table_name}:\n")
                    f.write("-" * len(table_name) + ":\n")
                    for field in table_df.schema.fields:
                        f.write(f"  {field.name}: {field.dataType}\n")
                    f.write("\n")
                except:
                    f.write(f"{table_name}: Could not read schema\n\n")
        
        spark.stop()
        print("Gold samples generation completed")
        
    except Exception as e:
        print(f"Could not generate Gold samples: {e}")
        print("This is likely because Gold layer hasn't been run yet or Spark/Delta dependencies are missing")

def generate_all_samples():
    """Generate all layer-specific sample data."""
    print("Generating layer-specific test fixtures...")
    print("=" * 50)
    
    # Generate Bronze sample (always works)
    print("\n1. Generating Bronze layer sample...")
    bronze_df = generate_bronze_sample()
    
    # Generate Silver sample (if Silver layer exists)
    print("\n2. Generating Silver layer sample...")
    silver_df = generate_silver_sample()
    
    # Generate Gold samples (if Gold layer exists)
    print("\n3. Generating Gold layer samples...")
    generate_gold_samples()
    
    print("\n" + "=" * 50)
    print("Sample generation completed!")
    print("\nGenerated fixtures:")
    print("- tests/fixtures/bronze_sample_train_data.csv (Bronze layer)")
    if silver_df is not None:
        print("- tests/fixtures/silver_sample_train_data.parquet (Silver layer)")
        print("- tests/fixtures/silver_schema_info.txt (Silver schema)")
    print("- tests/fixtures/gold/ (Gold layer tables)")
    print("- tests/fixtures/gold/gold_schemas_info.txt (Gold schemas)")
    
    # Show diversity stats
    if bronze_df is not None:
        print(f"\nBronze sample diversity:")
        print(f"  Unique lines: {bronze_df['Line_Name'].nunique()}")
        print(f"  Unique stations: {bronze_df['Station_Name'].nunique()}")
        print(f"  Unique dates: {bronze_df['Business_Date'].nunique()}")

if __name__ == "__main__":
    generate_all_samples()
