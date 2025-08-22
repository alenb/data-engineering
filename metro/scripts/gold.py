"""
Gold Layer - Business Intelligence and Star Schema

This module creates a dimensional data model (star schema) from the Silver layer data,
generating fact and dimension tables optimized for business intelligence and reporting.
"""

from scripts.base import Base
from pyspark.sql import functions as F


class Gold(Base):
    """Gold layer processor for creating star schema dimensional model."""

    def __init__(self):
        super().__init__()
        self.df = None
        self.dim_date = None
        self.dim_time_bucket = None
        self.dim_train = None
        self.dim_station = None
        self.dim_line = None
        self.dim_direction = None
        self.dim_mode = None
        self.fact_train = None

    def run(self) -> None:
        """Execute the complete Gold layer processing pipeline."""
        self.logger.info("Running Gold layer")
        self.create_spark()
        self.load_data()
        self.create_dim_date()
        self.create_dim_time_bucket()
        self.create_dim_train()
        self.create_dim_station()
        self.create_dim_line()
        self.create_dim_direction()
        self.create_dim_mode()
        self.create_fact_train()
        self.process()

    def load_data(self) -> None:
        """Load processed data from the Silver layer."""
        self.logger.info("Loading train patronage data")
        self.df = self.spark.read.format("delta").load(
            f"{self.config.SILVER_DATA_PATH}/train_patrons"
        )

    def create_dim_date(self) -> None:
        """Create the date dimension table with calendar attributes."""
        self.logger.info("Create dim_date")
        self.dim_date = (
            self.df.select(
                "Business_Date", "Day_of_Week", "Day_Type", "year", "month", "day"
            )
            .distinct()
            .withColumn(
                "Date_Key",
                F.date_format(F.col("Business_Date"), "yyyyMMdd").cast("int"),
            )
            .select(
                "Date_Key",
                "Business_Date",
                "Day_of_Week",
                "Day_Type",
                "year",
                "month",
                "day",
            )
            .orderBy("Business_Date")
        )

    def create_dim_time_bucket(self) -> None:
        """Create the time bucket dimension for 30-minute intervals."""

    def create_dim_time_bucket(self):
        self.logger.info("Create dim_time_bucket")
        arrival_buckets = (
            self.df.select("Arrival_Time_Bucket")
            .distinct()
            .filter(F.col("Arrival_Time_Bucket").isNotNull())
        )
        departure_buckets = (
            self.df.select("Departure_Time_Bucket")
            .distinct()
            .filter(F.col("Departure_Time_Bucket").isNotNull())
        )
        all_buckets = arrival_buckets.union(departure_buckets).distinct()
        self.dim_time_bucket = (
            all_buckets.withColumnRenamed("Arrival_Time_Bucket", "Time_Bucket")
            .withColumn("Time_Bucket_Key", F.col("Time_Bucket").cast("int"))
            .withColumn(
                "Time_Range_Description",
                F.concat(
                    F.format_string(
                        "%02d:%02d",
                        (F.col("Time_Bucket") / 60).cast("int"),
                        (F.col("Time_Bucket") % 60).cast("int"),
                    ),
                    F.lit(" - "),
                    F.format_string(
                        "%02d:%02d",
                        (F.col("Time_Bucket") / 60).cast("int"),
                        ((F.col("Time_Bucket") + 29) / 60).cast("int"),
                        ((F.col("Time_Bucket") + 29) % 60).cast("int"),
                    ),
                ),
            )
            .select("Time_Bucket_Key", "Time_Bucket", "Time_Range_Description")
            .orderBy("Time_Bucket")
        )

    def create_dim_train(self) -> None:
        """Create the train dimension with unique train attributes."""
        self.logger.info("Create dim_train")
        self.dim_train = (
            self.df.select(
                "Train_Number",
                "Mode",
                "Line_Name",
                "Group",
                "Direction",
                "Origin_Station",
                "Destination_Station",
            )
            .distinct()
            .withColumn(
                "Train_Key",
                F.abs(F.hash(F.concat_ws("||", F.col("Train_Number"), F.col("Mode")))),
            )
            .select(
                "Train_Key",
                "Train_Number",
                "Mode",
                "Line_Name",
                "Group",
                "Direction",
                "Origin_Station",
                "Destination_Station",
            )
            .orderBy("Train_Number")
        )

    def create_dim_station(self) -> None:
        """Create the station dimension with geographic information."""
        self.logger.info("Create dim_station")
        self.dim_station = (
            self.df.select(
                "Station_Name",
                "Station_Latitude",
                "Station_Longitude",
                "Station_Chainage",
            )
            .distinct()
            .withColumn("Station_Key", F.abs(F.hash(F.col("Station_Name"))))
            .select(
                "Station_Key",
                "Station_Name",
                "Station_Latitude",
                "Station_Longitude",
                "Station_Chainage",
            )
            .orderBy("Station_Name")
        )

    def create_dim_line(self) -> None:
        """Create the line dimension with railway line information."""
        self.logger.info("Create dim_line")
        self.dim_line = (
            self.df.select("Line_Name", "Group")
            .distinct()
            .withColumn("Line_Key", F.abs(F.hash(F.col("Line_Name"))))
            .select(
                "Line_Key",
                "Line_Name",
                "Group",
            )
            .orderBy("Line_Name")
        )

    def create_dim_direction(self) -> None:
        """Create the direction dimension with descriptive labels."""
        self.logger.info("Create dim_direction")
        self.dim_direction = (
            self.df.select("Direction")
            .distinct()
            .withColumn("Direction_Key", F.abs(F.hash(F.col("Direction"))))
            .withColumn(
                "Direction_Description",
                F.when(
                    F.col("Direction") == "U",
                    "Travelling towards Flinders Street Station",
                )
                .when(
                    F.col("Direction") == "D",
                    "Travelling away from Flinders Street Station",
                )
                .otherwise("Unknown Direction"),
            )
            .select("Direction_Key", "Direction", "Direction_Description")
            .orderBy("Direction")
        )

    def create_dim_mode(self) -> None:
        """Create the mode dimension for transport types."""
        self.logger.info("Create dim_mode")
        self.dim_mode = (
            self.df.select("Mode")
            .distinct()
            .withColumn("Mode_Key", F.abs(F.hash(F.col("Mode"))))
            .select("Mode_Key", "Mode")
            .orderBy("Mode")
        )

    def create_fact_train(self) -> None:
        """Create the central fact table with foreign keys to all dimensions."""
        self.logger.info("Create fact_train")
        fact_df = self.df.join(
            self.dim_date.select("Date_Key", "Business_Date"),
            on="Business_Date",
            how="left",
        )
        fact_df = fact_df.join(
            self.dim_time_bucket.select(
                F.col("Time_Bucket_Key").alias("Arrival_Time_Bucket_Key"),
                F.col("Time_Bucket").alias("Arrival_Time_Bucket_DTB"),
            ),
            fact_df["Arrival_Time_Bucket"] == F.col("Arrival_Time_Bucket_DTB"),
            how="left",
        )
        fact_df = fact_df.join(
            self.dim_time_bucket.select(
                F.col("Time_Bucket_Key").alias("Departure_Time_Bucket_Key"),
                F.col("Time_Bucket").alias("Departure_Time_Bucket_DTB"),
            ),
            fact_df["Departure_Time_Bucket"] == F.col("Departure_Time_Bucket_DTB"),
            how="left",
        )
        fact_df = fact_df.join(
            self.dim_train.select("Train_Key", "Train_Number", "Mode"),
            on=["Train_Number", "Mode"],
            how="left",
        )
        fact_df = fact_df.join(
            self.dim_station.select("Station_Key", "Station_Name"),
            on="Station_Name",
            how="left",
        )
        fact_df = fact_df.join(
            self.dim_line.select("Line_Key", "Line_Name"), on="Line_Name", how="left"
        )
        fact_df = fact_df.join(
            self.dim_mode.select("Mode_Key", "Mode"), on="Mode", how="left"
        )
        fact_df = fact_df.join(
            self.dim_direction.select("Direction_Key", "Direction"),
            on="Direction",
            how="left",
        )
        self.fact_train = (
            fact_df.select(
                "Date_Key",
                "Train_Key",
                "Station_Key",
                "Line_Key",
                "Direction_Key",
                "Mode_Key",
                "Stop_Sequence_Number",
                "Arrival_Time_Scheduled",
                "Departure_Time_Scheduled",
                "Arrival_Time_Bucket_Key",
                "Departure_Time_Bucket_Key",
                "Passenger_Boardings",
                "Passenger_Alightings",
                "Passenger_Arrival_Load",
                "Passenger_Departure_Load",
            )
            .withColumn(
                "Fact_ID",
                F.abs(
                    F.hash(
                        F.concat_ws(
                            "||",
                            F.col("Date_Key"),
                            F.col("Train_Key"),
                            F.col("Station_Key"),
                            F.col("Stop_Sequence_Number"),
                        )
                    )
                ),
            )
            .select(
                "Fact_ID",
                "Date_Key",
                "Train_Key",
                "Station_Key",
                "Line_Key",
                "Direction_Key",
                "Mode_Key",
                "Stop_Sequence_Number",
                "Arrival_Time_Scheduled",
                "Departure_Time_Scheduled",
                "Arrival_Time_Bucket_Key",
                "Departure_Time_Bucket_Key",
                "Passenger_Boardings",
                "Passenger_Alightings",
                "Passenger_Arrival_Load",
                "Passenger_Departure_Load",
            )
            .orderBy("Date_Key", "Train_Key", "Station_Key", "Stop_Sequence_Number")
        )

    def process(self) -> None:
        """Save all dimension and fact tables as Delta format."""
        self.logger.info("Saving gold data")
        self.dim_date.write.format("delta").mode("overwrite").save(
            f"{self.config.GOLD_DATA_PATH}/dim_date"
        )
        self.dim_time_bucket.write.format("delta").mode("overwrite").save(
            f"{self.config.GOLD_DATA_PATH}/dim_time_bucket"
        )
        self.dim_train.write.format("delta").mode("overwrite").save(
            f"{self.config.GOLD_DATA_PATH}/dim_train"
        )
        self.dim_station.write.format("delta").mode("overwrite").save(
            f"{self.config.GOLD_DATA_PATH}/dim_station"
        )
        self.dim_line.write.format("delta").mode("overwrite").save(
            f"{self.config.GOLD_DATA_PATH}/dim_line"
        )
        self.dim_direction.write.format("delta").mode("overwrite").save(
            f"{self.config.GOLD_DATA_PATH}/dim_direction"
        )
        self.dim_mode.write.format("delta").mode("overwrite").save(
            f"{self.config.GOLD_DATA_PATH}/dim_mode"
        )
        self.fact_train.write.format("delta").mode("overwrite").save(
            f"{self.config.GOLD_DATA_PATH}/fact_train"
        )

        self.spark.stop()
