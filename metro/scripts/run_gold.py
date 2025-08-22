"""
Gold Layer Runner

Executes the Gold layer data processing pipeline for creating
dimensional star schema tables from Silver layer data.
"""

from scripts.gold import Gold

if __name__ == "__main__":
    gold = Gold()
    gold.run()
