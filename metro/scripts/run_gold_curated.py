"""
Gold Curated Layer Runner

Executes the Gold curated layer processing pipeline for creating
specialised business analysis tables from Gold layer data.
"""

from scripts.gold_curated import GoldCurated

if __name__ == "__main__":
    gold_curated = GoldCurated()
    gold_curated.run()
