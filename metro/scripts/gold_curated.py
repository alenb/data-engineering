from scripts.base import Base
from pyspark.sql.window import Window
from pyspark.sql import functions as F

"""
Gold curated tables.
This module creates curated tables for analysis of passenger flow and station performance.
"""


class GoldCurated(Base):
    def __init__(self):
        super().__init__()
        self.station_peaks = None
        self.network_trend = None
        self.high_load_incidents = None

    """
    Run the Gold curated tables.
    """

    def run(self):
        self.logger.info("Running Gold curated tables")
        self.create_spark()
        self.load_tables()
        self.create_station_peaks()
        self.create_network_trend()
        self.create_high_load_incidents()
        self.process()

    """
    Load the necessary tables for Gold curated processing.
    """

    def load_tables(self):
        self.logger.info("Loading tables for Gold curated processing")
        fact_train = self.spark.read.format("delta").load(
            f"{self.config.GOLD_DATA_PATH}/fact_train"
        )
        dim_station = self.spark.read.format("delta").load(
            f"{self.config.GOLD_DATA_PATH}/dim_station"
        )
        dim_time_bucket = self.spark.read.format("delta").load(
            f"{self.config.GOLD_DATA_PATH}/dim_time_bucket"
        )
        dim_date = self.spark.read.format("delta").load(
            f"{self.config.GOLD_DATA_PATH}/dim_date"
        )

        fact_train.createOrReplaceTempView("fact_train")
        dim_station.createOrReplaceTempView("dim_station")
        dim_time_bucket.createOrReplaceTempView("dim_time_bucket")
        dim_date.createOrReplaceTempView("dim_date")

    """
    Create station AM/PM peaks table which allows for analysis of passenger flow during peak hours.
    """

    def create_station_peaks(self):
        self.logger.info("Calculating station peaks")
        self.station_peaks = self.spark.sql(
            """
            WITH daily_totals AS (
                SELECT
                    s.Station_Key,
                    s.Station_Name,
                    d.Business_Date,
                    SUM(CASE WHEN tb.Time_Bucket BETWEEN 420 AND 539 THEN f.Passenger_Boardings END) AS AM_Boardings,
                    SUM(CASE WHEN tb.Time_Bucket BETWEEN 420 AND 539 THEN f.Passenger_Alightings END) AS AM_Alightings,
                    SUM(CASE WHEN tb.Time_Bucket BETWEEN 960 AND 1079 THEN f.Passenger_Boardings END) AS PM_Boardings,
                    SUM(CASE WHEN tb.Time_Bucket BETWEEN 960 AND 1079 THEN f.Passenger_Alightings END) AS PM_Alightings
                FROM fact_train f
                JOIN dim_station s ON f.Station_Key = s.Station_Key
                JOIN dim_time_bucket tb ON f.Departure_Time_Bucket_Key = tb.Time_Bucket_Key
                JOIN dim_date d ON f.Date_Key = d.Date_Key
                WHERE d.Day_Type = 'Normal Weekday'
                GROUP BY s.Station_Key, s.Station_Name, d.Business_Date
            )
            SELECT
                Station_Key,
                Station_Name,
                ROUND(COALESCE(AVG(AM_Boardings),0),1) AS Avg_AM_Boardings,
                ROUND(COALESCE(AVG(AM_Alightings),0),1) AS Avg_AM_Alightings,
                ROUND(COALESCE(AVG(PM_Boardings),0),1) AS Avg_PM_Boardings,
                ROUND(COALESCE(AVG(PM_Alightings),0),1) AS Avg_PM_Alightings,
                COALESCE(SUM(AM_Boardings),0) AS Total_AM_Boardings,
                COALESCE(SUM(AM_Alightings),0) AS Total_AM_Alightings,
                COALESCE(SUM(PM_Boardings),0) AS Total_PM_Boardings,
                COALESCE(SUM(PM_Alightings),0) AS Total_PM_Alightings,
                '07:00' AS AM_Peak_From_Time,
                '09:00' AS AM_Peak_To_Time,
                '16:00' AS PM_Peak_From_Time,
                '18:00' AS PM_Peak_To_Time
            FROM daily_totals
            GROUP BY Station_Key, Station_Name
            """
        )

    """
    Create network trend table which allows for analysis of passenger flow over time.
    """

    def create_network_trend(self):
        self.logger.info("Calculating network trend")

        daily_totals = self.spark.sql(
            """
            WITH date_bounds AS (
                SELECT MIN(Business_Date) AS MinDate, MAX(Business_Date) AS MaxDate
                FROM dim_date
            )
            SELECT
                d.Business_Date,
                SUM(f.Passenger_Boardings) AS Total_Boardings,
                SUM(f.Passenger_Alightings) AS Total_Alightings,
                MONTH(d.Business_Date) AS month
            FROM fact_train f
            JOIN dim_date d ON f.Date_Key = d.Date_Key
            CROSS JOIN date_bounds
            WHERE d.Business_Date BETWEEN MinDate AND MaxDate
            GROUP BY d.Business_Date
            """
        )

        # Define window over month for rolling averages
        w = Window.partitionBy("month").orderBy("Business_Date").rowsBetween(-6, 0)

        self.network_trend = (
            daily_totals.withColumn(
                "Rolling7_Boardings",
                F.round(F.avg("Total_Boardings").over(w), 0).cast("long"),
            )
            .withColumn(
                "Rolling7_Alightings",
                F.round(F.avg("Total_Alightings").over(w), 0).cast("long"),
            )
            .withColumn(
                "Boardings_Flag",
                F.when(
                    F.abs(F.col("Total_Boardings") - F.col("Rolling7_Boardings"))
                    / F.col("Rolling7_Boardings")
                    >= 0.2,
                    "Deviation",
                ).otherwise("Normal"),
            )
            .withColumn(
                "Alightings_Flag",
                F.when(
                    F.abs(F.col("Total_Alightings") - F.col("Rolling7_Alightings"))
                    / F.col("Rolling7_Alightings")
                    >= 0.2,
                    "Deviation",
                ).otherwise("Normal"),
            )
        )

        # Drop helper column
        self.network_trend = self.network_trend.drop("month")

    """
    Create high load incidents table which allows for analysis of incidents at stations with high passenger loads.
    """

    def create_high_load_incidents(self):
        self.logger.info("Identifying high load incidents")
        self.high_load_incidents = self.spark.sql(
            """
            WITH max_load AS (
                SELECT MAX(Passenger_Departure_Load) AS MaxLoad
                FROM fact_train
            )
            SELECT
                d.Business_Date,
                s.Station_Key,
                s.Station_Name,
                COUNT(*) AS Incident_Count,
                MAX(f.Passenger_Departure_Load) AS Max_Departure_Load
            FROM fact_train f
            JOIN dim_station s ON f.Station_Key = s.Station_Key
            JOIN dim_date d ON f.Date_Key = d.Date_Key
            CROSS JOIN max_load
            WHERE f.Passenger_Departure_Load > MaxLoad * 0.75
            GROUP BY d.Business_Date, s.Station_Key, s.Station_Name
            ORDER BY d.Business_Date, Incident_Count DESC
            """
        )

    """
    Process the DataFrame and store the results in Delta format.
    """

    def process(self):
        self.logger.info("Saving gold curated data")
        self.station_peaks.write.format("delta").mode("overwrite").save(
            f"{self.config.GOLD_DATA_PATH}/station_peaks"
        )
        self.network_trend.write.format("delta").mode("overwrite").save(
            f"{self.config.GOLD_DATA_PATH}/network_trend"
        )
        self.high_load_incidents.write.format("delta").mode("overwrite").save(
            f"{self.config.GOLD_DATA_PATH}/high_load_incidents"
        )

        # Stop the Spark session
        self.spark.stop()