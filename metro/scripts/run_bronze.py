"""
Bronze Layer Runner

Executes the Bronze layer data processing pipeline for ingesting
raw train patronage data from the DataVic API.
"""

from scripts.bronze import Bronze

if __name__ == "__main__":
    bronze = Bronze()
    bronze.run()
