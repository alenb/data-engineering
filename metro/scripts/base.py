import subprocess
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from config.config import Config
from scripts.utils.logger import Logger

"""
Base class for all processing layers.
"""


class Base:
    def __init__(self):
        self.logger = Logger()
        self.config = Config()
        self.spark = None

    """
    Create a Spark session.
    """

    def create_spark(self):
        self.logger.info("Starting Spark session")
        builder = (
            SparkSession.builder.config(
                "spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"
            )
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
            .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0")
            .config("spark.driver.memory", "4g")
            .config("spark.executor.memory", "4g")
            .config("spark.memory.fraction", "0.6")
            .config("spark.memory.storageFraction", "0.5")
        )
        # Configure Spark with Delta Lake
        self.spark = configure_spark_with_delta_pip(builder).getOrCreate()

    """
    Download a file from a URL.
    """

    def download(self, url, filename):
        """Download using aria2 - the most robust downloader for large files"""
        self.config.BRONZE_DATA_PATH.mkdir(parents=True, exist_ok=True)

        # aria2 command with maximum reliability settings
        cmd = [
            "aria2c",
            "--continue=true",  # Resume downloads
            "--max-tries=0",  # Infinite retries
            "--retry-wait=10",  # Wait 10 seconds between retries
            "--timeout=60",  # 60 second timeout per connection
            "--max-connection-per-server=4",  # 4 connections to server
            "--split=4",  # Split into 4 segments
            "--min-split-size=50M",  # Minimum 50MB per segment
            "--max-download-limit=0",  # No speed limit
            "--file-allocation=prealloc",  # Pre-allocate file space
            "--check-integrity=true",  # Verify download integrity
            "--auto-file-renaming=false",  # Don't rename if file exists
            "--allow-overwrite=true",  # Overwrite existing files
            "--dir",
            str(self.config.BRONZE_DATA_PATH),
            "--out",
            filename,
            "--log-level=info",
            "--summary-interval=10",  # Progress every 10 seconds
            url,
        ]

        try:
            self.logger.info(f"Starting aria2 download: {' '.join(cmd)}")

            # Run aria2 and capture output
            process = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                check=False,  # Don't raise on non-zero exit
            )

            if process.returncode == 0:
                file_path = self.config.BRONZE_DATA_PATH / filename
                file_size = file_path.stat().st_size
                self.logger.info(
                    f"aria2 download completed: {file_path} ({file_size:,} bytes)"
                )
            else:
                self.logger.error(f"aria2 failed with return code {process.returncode}")
                self.logger.error(f"stderr: {process.stderr}")
                raise Exception(f"aria2 download failed: {process.stderr}")

        except FileNotFoundError:
            self.logger.error("aria2c not found. Please install aria2.")
            raise
        except Exception as e:
            self.logger.error(f"aria2 download failed: {e}")
            raise
