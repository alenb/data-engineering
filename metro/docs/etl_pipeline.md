
# Metro ETL Pipeline: Bronze → Silver → Gold

This document provides a step-by-step walkthrough of the ETL process powering the Metro data platform. Each layer—Bronze, Silver, and Gold—has a distinct role, and together they transform raw public transit data into analytics-ready tables. Here's what happens behind the scenes:

## Bronze Layer: Raw Data Ingestion

The Bronze layer is the first stop for data. Here, the `bronze.py` script fetches train patronage data directly from the Datavic API. It:

- Connects to the API endpoint using secure credentials.
- Downloads the latest train service passenger counts in CSV format.
- Stores the file in the bronze data directory, preserving the raw structure and all original fields.

This layer acts as a historical archive, capturing every record—errors, quirks, and all. No cleaning or transformation is performed at this stage; the goal is to ensure nothing is lost and that the original data is always available for reference or reprocessing.

## Silver Layer: Cleansing and Transformation

The Silver layer, managed by `silver.py`, is where the data gets its first polish. The process includes:

- **Loading:** Reads the raw CSV data from the Bronze layer.
- **Cleaning:** Negative passenger counts are replaced with nulls to avoid misleading analytics.
- **Time Features:** Extracts arrival and departure times, then creates 30-minute time buckets for each event.
- **Data Enrichment:** Fills missing 'Group' values using the 'Line_Name' field, and adds derived date parts (year, month, day) for partitioning and analysis.
- **Imputation:** Uses window functions to fill missing passenger values by carrying forward the last valid observation within each train's sequence. Special handling ensures the first stop's missing values are sensibly filled.
- **Partitioning and Storage:** Writes the cleaned and enriched data to the silver directory in Delta format, partitioned by year, month, and day for efficient querying.

The Silver layer transforms chaos into order, producing structured tables that are ready for analysis and further refinement.

## Gold Layer: Analytics and Star Schema Modelling

The Gold layer, orchestrated by `gold.py`, aggregates and models the data for business intelligence. Key steps include:

- **Loading:** Reads the curated train patronage data from the Silver layer.
- **Dimension Creation:** Builds dimension tables for date, time bucket, train, station, line, direction, and mode. Each dimension is assigned a surrogate key for fast joins and clear relationships.
- **Fact Table Construction:** Joins all dimensions to create the `fact_train` table, which records each train stop event, including passenger counts, scheduled times, and all relevant keys.
- **Star Schema Output:** Stores each dimension and the fact table in the gold directory in Delta format, ready for reporting and analytics.

The Gold layer is the final destination, where data is shaped into a star schema optimised for slicing, dicing, and surfacing insights.

## Tools and Orchestration

- **Apache Spark:** Handles distributed processing and transformations at scale.
- **Delta Lake:** Ensures ACID compliance, reliability, and efficient storage.

Each layer is stored in Delta format, supporting scalable queries and time travel for auditing and recovery.