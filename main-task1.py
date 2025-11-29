"""
main_task1.py
Top-level runner for Task 1 pipeline
Scrape -> Validate -> Clean -> Quality Checks
"""

import os
from subprocess import run
from pathlib import Path

ROOT = Path(__file__).resolve().parent

def run_script(path):
    print(f"\n>>> Running: {path}")
    res = run(["python", str(path)], check=False)
    if res.returncode != 0:
        print(f"❌ Script returned exit code {res.returncode}")

if __name__ == "__main__":
    # 1. Scrape raw reviews for all banks
    run_script(ROOT / "2_data_pipeline" / "data_collection" / "scraper.py")

    # 2. Validate raw data
    run_script(ROOT / "2_data_pipeline" / "data_collection" / "data_validation.py")

    # 3. Clean data
    run_script(ROOT / "2_data_pipeline" / "data_processing" / "data_cleaning.py")

    # 4. Data quality checks
    run_script(ROOT / "2_data_pipeline" / "data_processing" / "quality_checks.py")

    print("\n🎉 Task-1 pipeline completed successfully!")
