"""
Silver Layer Runner

Executes the Silver layer data processing pipeline for cleaning
and transforming Bronze layer data.
"""

from scripts.silver import Silver

if __name__ == "__main__":
    silver = Silver()
    silver.run()
