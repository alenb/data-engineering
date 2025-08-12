from base import Base
from pyspark.sql.window import Window
from pyspark.sql.functions import (
    col,
    when,
    coalesce,
    lag,
    date_format,
    expr,
    year,
    month,
    dayofmonth,
)

"""
Silver layer for processing train patronage data.
This script processes the data from the Bronze layer, cleaning and transforming it.
"""


class Silver(Base):
    def __init__(self):
        super().__init__()
        self.train_patron = None
        self.df = None

    """
    Run the Silver layer processing.
    """

    def run(self):
        self.load_data()
        self.clean_negative_passenger_counts()
        self.extract_time_of_day_info()
        self.fill_group_with_line_name()
        self.add_derived_date_parts()
        self.fill_missing_passenger_values()
        self.process()

    """
    Load train patronage data from the Bronze layer.
    """

    def load_data(self):
        self.logger.info("Loading train patronage data")
        self.train_patron = self.spark.read.csv(
            f"{self.config.BRONZE_DATA_PATH}/train_patrons.csv",
            header=True,
            inferSchema=True,
        )

    """
    Clean negative passenger counts by replacing them with null.
    """

    def clean_negative_passenger_counts(self):
        self.logger.info("Cleaning negative counts")
        self.df = (
            self.train_patron.withColumn(
                "Passenger_Boardings",
                when(col("Passenger_Boardings") < 0, None).otherwise(
                    col("Passenger_Boardings")
                ),
            )
            .withColumn(
                "Passenger_Alightings",
                when(col("Passenger_Alightings") < 0, None).otherwise(
                    col("Passenger_Alightings")
                ),
            )
            .withColumn(
                "Passenger_Arrival_Load",
                when(col("Passenger_Arrival_Load") < 0, None).otherwise(
                    col("Passenger_Arrival_Load")
                ),
            )
            .withColumn(
                "Passenger_Departure_Load",
                when(col("Passenger_Departure_Load") < 0, None).otherwise(
                    col("Passenger_Departure_Load")
                ),
            )
        )

    """
    Extract time-of-day information from the train patronage data, and create time buckets.
    """

    def extract_time_of_day_info(self):
        self.logger.info("Extracting time-of-day information")
        self.df = (
            self.train_patron.withColumn(
                "Arrival_Time", date_format(col("Arrival_Time_Scheduled"), "HH:mm:ss")
            )
            .withColumn(
                "Departure_Time",
                date_format(col("Departure_Time_Scheduled"), "HH:mm:ss"),
            )
            .withColumn(
                "Arrival_Time_Bucket",
                expr(
                    "floor((hour(Arrival_Time_Scheduled) * 60 + minute(Arrival_Time_Scheduled)) / 30) * 30"
                ),
            )
            .withColumn(
                "Departure_Time_Bucket",
                expr(
                    "floor((hour(Departure_Time_Scheduled) * 60 + minute(Departure_Time_Scheduled)) / 30) * 30"
                ),
            )
        )

    """
    Fill Group with Line_Name where Group is null.
    """

    def fill_group_with_line_name(self):
        self.logger.info("Filling Group with Line_Name where Group is null")
        self.df = self.df.withColumn(
            "Group",
            when(
                (col("Group").isNull()) & (col("Line_Name").isNotNull()),
                col("Line_Name"),
            ).otherwise(col("Group")),
        )

    """
    Add derived date parts for partitioning and analysis.
    """

    def add_derived_date_parts(self):
        self.logger.info("Adding derived date parts for partitioning and analysis")
        self.df = (
            self.df.withColumn("year", year(col("Business_Date")))
            .withColumn("month", month(col("Business_Date")))
            .withColumn("day", dayofmonth(col("Business_Date")))
        )

    """
    Fill missing passenger values by carrying forward the last observation or using the next valid observation.
    """

    def fill_missing_passenger_values(self):
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
            prev_col = lag(col(col_name)).over(window_spec)
            self.df = self.df.withColumn(
                f"{col_name}_filled", coalesce(col(col_name), prev_col)
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
            when(
                (col("Stop_Sequence_Number") == 1)
                & (col("Passenger_Boardings").isNull()),
                col("Passenger_Alightings"),
            ).otherwise(col("Passenger_Boardings")),
        ).withColumn(
            "Passenger_Departure_Load",
            when(
                (col("Stop_Sequence_Number") == 1)
                & (col("Passenger_Departure_Load").isNull()),
                col("Passenger_Arrival_Load"),
            ).otherwise(col("Passenger_Departure_Load")),
        )

        self.df = self.df.withColumn(
            "Group",
            when(
                (col("Line_Name") == "Stony Point") & (col("Group").isNull()),
                "Stony Point",
            ).otherwise(col("Group")),
        )

    """
    Process the DataFrame and store the results in Delta format.
    """

    def process(self):
        # Store the silver table as delta partitioned by year/month/day
        self.logger.info(
            "Storing the silver table as delta partitioned by year/month/day"
        )
        self.df.write.format("delta").mode("overwrite").partitionBy(
            "year", "month", "day"
        ).save(f"{self.config.SILVER_DATA_PATH}/train_patrons")
        self.logger.info(
            f"Processing completed successfully: {self.config.SILVER_DATA_PATH}/train_patrons"
        )

        # Stop the Spark session
        self.spark.stop()


"""
Entry point for the script
"""
if __name__ == "__main__":
    silver = Silver()
    silver.run()
