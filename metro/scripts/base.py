"""
Base class for all data processing layers.

This module provides common functionality for Bronze, Silver, and Gold layer processing,
including Spark session management and file download capabilities.
"""

import subprocess
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from config.config import Config
from scripts.utils.logger import Logger


class Base:
    """Base class providing common functionality for all processing layers."""

    def __init__(self):
        self.logger = Logger()
        self.config = Config()
        self.spark = None

    def create_spark(self) -> None:
        """Create and configure a Spark session with Delta Lake support."""
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
        self.spark = configure_spark_with_delta_pip(builder).getOrCreate()

    def download(self, url: str, filename: str) -> None:
        """
        Download a file from a URL using aria2 for robust large file downloads.

        Args:
            url: The URL to download from
            filename: The local filename to save as
        """
        """Download using aria2 - the most robust downloader for large files"""
        self.config.BRONZE_DATA_PATH.mkdir(parents=True, exist_ok=True)

        cmd = [
            "aria2c",
            "--continue=true",
            "--max-tries=0",
            "--retry-wait=10",
            "--timeout=60",
            "--max-connection-per-server=4",
            "--split=4",
            "--min-split-size=50M",
            "--max-download-limit=0",
            "--file-allocation=prealloc",
            "--check-integrity=true",
            "--auto-file-renaming=false",
            "--allow-overwrite=true",
            "--dir",
            str(self.config.BRONZE_DATA_PATH),
            "--out",
            filename,
            "--log-level=info",
            "--summary-interval=10",
            url,
        ]

        try:
            self.logger.info(f"Starting aria2 download: {' '.join(cmd)}")

            process = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                check=False,
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
