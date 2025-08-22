"""
Bronze Layer - Raw Data Ingestion

This module handles the Bronze layer of the data pipeline, responsible for
ingesting raw train patronage data from the DataVic API and storing it
in its natural format for further processing.
"""

import requests
from scripts.base import Base


class Bronze(Base):
    """Bronze layer processor for raw data ingestion."""

    def __init__(self):
        super().__init__()

    def run(self) -> None:
        """Execute the Bronze layer processing pipeline."""
        self.logger.info("Running Bronze layer")
        self.get_train_patronage()

    def get_train_patronage(self) -> None:
        """
        Retrieve train patronage data from the DataVic API.

        Fetches metadata about the train patronage dataset and downloads
        the actual data file to the bronze data directory.
        """
        endpoint = f"{self.config.DATAVIC_BASE_URL}/resource_show?id=162887ef-1dba-4d9b-83bd-baee229229c6"
        headers = {"accept": "application/json", "apikey": self.config.DATAVIC_API_KEY}

        response = requests.get(endpoint, headers=headers, timeout=30)

        if response.status_code == 200:
            data = response.json()
            resource = data.get("result", {})
            download_url = resource.get("url")

            if not download_url:
                raise ValueError(
                    "Train service passenger counts download url not found."
                )

            self.logger.info(
                f"Downloading train service passenger counts from {download_url}"
            )

            file_format = resource.get("format", "csv").lower()
            filename = f"train_patrons.{file_format}"

            try:
                self.download(download_url, filename)
                self.logger.info("Download and save completed successfully")
            except Exception as e:
                self.logger.error(f"Download failed: {e}")
                raise

        else:
            raise ValueError("Train service passenger counts data could not be loaded.")
