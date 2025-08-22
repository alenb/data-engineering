"""
Silver Layer - Data Cleaning and Transformation

This module processes data from the Bronze layer, performing data cleaning,
validation, and structural transformations to prepare it for business analysis.
"""

from scripts.base import Base
from pyspark.sql.window import Window
from pyspark.sql import functions as F


class Silver(Base):
    """Silver layer processor for data cleaning and transformation."""

    def __init__(self):
        super().__init__()
        self.df = None

    def run(self) -> None:
        """Execute the complete Silver layer processing pipeline."""
        self.logger.info("Running Silver layer")
        self.create_spark()
        self.load_data()
        self.clean_negative_passenger_counts()
        self.fix_arrival_departure_times()
        self.extract_time_of_day_info()
        self.fill_group_with_line_name()
        self.add_derived_date_parts()
        self.fill_missing_passenger_values()
        self.process()

    def load_data(self) -> None:
        """Load train patronage data from the Bronze layer."""
        self.logger.info("Loading train patronage data")
        self.df = self.spark.read.csv(
            f"{self.config.BRONZE_DATA_PATH}/train_patrons.csv",
            header=True,
            inferSchema=True,
        )

    def clean_negative_passenger_counts(self) -> None:
        """Replace negative passenger counts with null values for data quality."""
        self.logger.info("Cleaning negative counts")
        self.df = (
            self.df.withColumn(
                "Passenger_Boardings",
                F.when(F.col("Passenger_Boardings") < 0, None).otherwise(
                    F.col("Passenger_Boardings")
                ),
            )
            .withColumn(
                "Passenger_Alightings",
                F.when(F.col("Passenger_Alightings") < 0, None).otherwise(
                    F.col("Passenger_Alightings")
                ),
            )
            .withColumn(
                "Passenger_Arrival_Load",
                F.when(F.col("Passenger_Arrival_Load") < 0, None).otherwise(
                    F.col("Passenger_Arrival_Load")
                ),
            )
            .withColumn(
                "Passenger_Departure_Load",
                F.when(F.col("Passenger_Departure_Load") < 0, None).otherwise(
                    F.col("Passenger_Departure_Load")
                ),
            )
        )

    def fix_arrival_departure_times(self) -> None:
        """
        Combine Business_Date with time columns to create proper timestamps.

        Creates unified timestamp columns by concatenating the business date
        with the scheduled arrival and departure times.
        """
        self.logger.info("Fixing arrival and departure times")
        self.df = (
            self.df.withColumn(
                "Arrival_Timestamp",
                F.to_timestamp(
                    F.concat_ws(
                        " ",
                        F.col("Business_Date"),
                        F.date_format(F.col("Arrival_Time_Scheduled"), "HH:mm:ss"),
                    )
                ),
            )
            .withColumn(
                "Departure_Timestamp",
                F.to_timestamp(
                    F.concat_ws(
                        " ",
                        F.col("Business_Date"),
                        F.date_format(F.col("Departure_Time_Scheduled"), "HH:mm:ss"),
                    )
                ),
            )
            .drop("Arrival_Time_Scheduled", "Departure_Time_Scheduled")
            .withColumnRenamed("Arrival_Timestamp", "Arrival_Time_Scheduled")
            .withColumnRenamed("Departure_Timestamp", "Departure_Time_Scheduled")
        )

    def extract_time_of_day_info(self) -> None:
        """
        Extract time-of-day information and create 30-minute time buckets.

        Adds time columns and creates time buckets for analysis of
        passenger patterns throughout the day.
        """
        self.logger.info("Extracting time-of-day information")
        self.df = (
            self.df.withColumn(
                "Arrival_Time",
                F.date_format(F.col("Arrival_Time_Scheduled"), "HH:mm:ss"),
            )
            .withColumn(
                "Departure_Time",
                F.date_format(F.col("Departure_Time_Scheduled"), "HH:mm:ss"),
            )
            .withColumn(
                "Arrival_Time_Bucket",
                F.expr(
                    "floor((hour(Arrival_Time_Scheduled) * 60 + minute(Arrival_Time_Scheduled)) / 30) * 30"
                ),
            )
            .withColumn(
                "Departure_Time_Bucket",
                F.expr(
                    "floor((hour(Departure_Time_Scheduled) * 60 + minute(Departure_Time_Scheduled)) / 30) * 30"
                ),
            )
        )

    def fill_group_with_line_name(self) -> None:
        """Fill missing Group values with Line_Name where possible."""
        self.logger.info("Filling Group with Line_Name where Group is null")
        self.df = self.df.withColumn(
            "Group",
            F.when(
                (F.col("Group").isNull()) & (F.col("Line_Name").isNotNull()),
                F.col("Line_Name"),
            ).otherwise(F.col("Group")),
        )

    def add_derived_date_parts(self) -> None:
        """Add year, month, and day columns for data partitioning."""
        self.logger.info("Adding derived date parts for partitioning and analysis")
        self.df = (
            self.df.withColumn("year", F.year(F.col("Business_Date")))
            .withColumn("month", F.month(F.col("Business_Date")))
            .withColumn("day", F.dayofmonth(F.col("Business_Date")))
        )

    def fill_missing_passenger_values(self) -> None:
        """
        Fill missing passenger values using forward fill and business logic.

        Uses window functions to carry forward last known values within
        train sequences and applies business rules for specific scenarios.
        """
        self.logger.info(
            "Finding previous values for passenger columns within the sequence to fill missing values"
        )
        passenger_columns = [
            "Passenger_Boardings",
            "Passenger_Alightings",
            "Passenger_Arrival_Load",
            "Passenger_Departure_Load",
        ]

        window_spec = Window.partitionBy(
            "Business_Date", "Train_Number", "Mode"
        ).orderBy("Stop_Sequence_Number")

        for col_name in passenger_columns:
            prev_col = F.lag(F.col(col_name)).over(window_spec)
            self.df = self.df.withColumn(
                f"{col_name}_filled", F.coalesce(F.col(col_name), prev_col)
            )

        for col_name in passenger_columns:
            self.df = self.df.drop(col_name).withColumnRenamed(
                f"{col_name}_filled", col_name
            )

        self.logger.info(
            "Filling missing values for passenger columns when Stop_Sequence_Number is 1"
        )
        self.df = self.df.withColumn(
            "Passenger_Boardings",
            F.when(
                (F.col("Stop_Sequence_Number") == 1)
                & (F.col("Passenger_Boardings").isNull()),
                F.col("Passenger_Alightings"),
            ).otherwise(F.col("Passenger_Boardings")),
        ).withColumn(
            "Passenger_Departure_Load",
            F.when(
                (F.col("Stop_Sequence_Number") == 1)
                & (F.col("Passenger_Departure_Load").isNull()),
                F.col("Passenger_Arrival_Load"),
            ).otherwise(F.col("Passenger_Departure_Load")),
        )

        self.df = self.df.withColumn(
            "Group",
            F.when(
                (F.col("Line_Name") == "Stony Point") & (F.col("Group").isNull()),
                "Stony Point",
            ).otherwise(F.col("Group")),
        )

    def process(self) -> None:
        """Save the processed DataFrame as Delta format with partitioning."""
        self.logger.info(
            "Storing the silver table as delta partitioned by year/month/day"
        )
        self.df.write.format("delta").mode("overwrite").partitionBy(
            "year", "month", "day"
        ).save(f"{self.config.SILVER_DATA_PATH}/train_patrons")
        self.logger.info(
            f"Processing completed successfully: {self.config.SILVER_DATA_PATH}/train_patrons"
        )

        self.spark.stop()
