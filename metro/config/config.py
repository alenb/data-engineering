"""
Configuration for the project.
"""

import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from config/.env
config_dir = Path(__file__).parent
env_file = config_dir / ".env"
load_dotenv(env_file)


class Config:
    # Project paths
    PROJECT_ROOT = Path(__file__).parent.parent
    DATA_ROOT = PROJECT_ROOT / "data"
    BRONZE_DATA_PATH = DATA_ROOT / "raw"
    SILVER_DATA_PATH = DATA_ROOT / "silver"
    GOLD_DATA_PATH = DATA_ROOT / "gold"
    LOGS_PATH = PROJECT_ROOT / "logs"

    # API Configuration
    PTV_USER_ID = os.getenv("PTV_USER_ID")
    PTV_API_KEY = os.getenv("PTV_API_KEY")
    PTV_BASE_URL = os.getenv("PTV_BASE_URL", "https://timetableapi.ptv.vic.gov.au")

    # Data.vic.gov.au API Configuration
    DATAVIC_API_KEY = os.getenv("DATAVIC_API_KEY")
    DATAVIC_SECRET_KEY = os.getenv("DATAVIC_SECRET_KEY")
    DATAVIC_BASE_URL = os.getenv(
        "DATAVIC_BASE_URL",
        "https://wovg-community.gateway.prod.api.vic.gov.au/datavic/v1.2",
    )

    # VicRoads API Configuration (for GTFS Realtime)
    VICROADS_API_KEY = os.getenv("VICROADS_API_KEY")

    # ABS API (free access - no API key required as of Nov 2024)
    ABS_BASE_URL = os.getenv("ABS_BASE_URL", "https://api.abs.gov.au")

    # Databricks Configuration
    DATABRICKS_HOST = os.getenv("DATABRICKS_HOST")
    DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")
    DATABRICKS_CLUSTER_ID = os.getenv("DATABRICKS_CLUSTER_ID")
