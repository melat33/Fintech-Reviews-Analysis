"""
2_data_pipeline/data_collection/scraper.py
Scrapes reviews for configured Google Play package IDs
Saves BOTH:
    - Combined raw CSV (all banks)
    - Individual CSV per bank
"""

import os
import time
import csv
import yaml
from tqdm import tqdm
from google_play_scraper import reviews, Sort

# ---------------------------------------------------------
# Paths - CORRECTED to use data/raw directory
# ---------------------------------------------------------
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))

CONFIG_PATH = os.path.join(ROOT, "3_configuration", "config.yaml")

# Use the raw subdirectory as requested
PROCESSED_DIR = os.path.join(ROOT, "2_data_pipeline", "data", "raw")
ALL_RAW_PATH = os.path.join(PROCESSED_DIR, "all_reviews.csv")

os.makedirs(PROCESSED_DIR, exist_ok=True)

# ---------------------------------------------------------
# Load configuration
# ---------------------------------------------------------
print("📁 Loading configuration...")
try:
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    print("✅ Configuration loaded successfully")
except Exception as e:
    print(f"❌ Error loading config: {e}")
    exit(1)

PACKAGE_MAP = cfg.get("package_map", {})
PER_BANK_TARGET = int(cfg.get("per_bank_target", 400))
SLEEP = float(cfg.get("sleep_between_requests", 1.5))
MAX_RETRIES = int(cfg.get("max_retries", 3))
SOURCE = "Google Play"

print(f"🎯 Target: {PER_BANK_TARGET} reviews per bank")
print(f"📱 Apps to scrape: {len(PACKAGE_MAP)}")

# ---------------------------------------------------------
# Scrape function
# ---------------------------------------------------------
def fetch_reviews_for_app(package_name, bank_name, target=PER_BANK_TARGET):
    all_reviews = []
    token = None
    retries = 0

    pbar = tqdm(total=target, desc=f"Scraping {bank_name}", unit="rev")

    while len(all_reviews) < target:
        try:
            results, token = reviews(
                package_name,
                lang="en",
                country="us",
                sort=Sort.NEWEST,
                count=200,
                continuation_token=token
            )
        except Exception as e:
            retries += 1
            print(f"[{bank_name}] fetch error: {e} (retry {retries}/{MAX_RETRIES})")

            if retries >= MAX_RETRIES:
                break

            time.sleep(SLEEP * 2)
            continue

        retries = 0

        if not results:
            break

        for r in results:
            row = {
                "review_id": r.get("reviewId"),
                "review": r.get("content"),
                "score": r.get("score"),
                "at": r.get("at").isoformat() if r.get("at") else None,
                "user_name": r.get("userName"),
                "reply_text": r.get("replyContent") or None,
                "reply_date": r.get("repliedAt").isoformat() if r.get("repliedAt") else None,
                "package_name": package_name,
                "bank_name": bank_name,
                "source": SOURCE
            }
            all_reviews.append(row)
            pbar.update(1)

            if len(all_reviews) >= target:
                break

        if not token:
            break

        time.sleep(SLEEP)

    pbar.close()
    return all_reviews

# ---------------------------------------------------------
# Save CSV
# ---------------------------------------------------------
def write_csv(path, rows):
    fieldnames = [
        "review_id", "review", "score", "at", "user_name",
        "reply_text", "reply_date", "package_name", "bank_name", "source"
    ]

    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"💾 Saved {len(rows)} reviews → {path}")

# ---------------------------------------------------------
# Main
# ---------------------------------------------------------
def main():
    print("\n🚀 STARTING GOOGLE PLAY REVIEW SCRAPER")
    print("=" * 50)
    
    all_banks_reviews = []

    for pkg, bank_name in PACKAGE_MAP.items():
        print(f"\n========== Scraping {bank_name} ==========")

        bank_reviews = fetch_reviews_for_app(pkg, bank_name, target=PER_BANK_TARGET)

        # Store per-bank file in the raw directory
        bank_file = os.path.join(PROCESSED_DIR, f"{bank_name.lower().replace(' ', '_')}_reviews.csv")
        write_csv(bank_file, bank_reviews)

        all_banks_reviews.extend(bank_reviews)

    # Write combined dataset
    write_csv(ALL_RAW_PATH, all_banks_reviews)

    print(f"\n🎉 Scraping completed for all banks.")
    print(f"📊 Total reviews saved: {len(all_banks_reviews)}")
    print(f"📁 All files saved to: {PROCESSED_DIR}")

if __name__ == "__main__":
    main()