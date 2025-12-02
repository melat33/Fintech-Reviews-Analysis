"""
2_data_pipeline/data_collection/scraper.py
Scrapes reviews for configured Google Play package IDs
"""

import os
import time
import csv
import yaml
from tqdm import tqdm
from google_play_scraper import reviews, Sort, app

# ---------------------------------------------------------
# Paths
# ---------------------------------------------------------
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
CONFIG_PATH = os.path.join(ROOT, "3_configuration", "config.yaml")
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
PER_BANK_TARGET = int(cfg.get("per_bank_target", 100))  # Reduced for testing
SLEEP = float(cfg.get("sleep_between_requests", 2.0))
MAX_RETRIES = int(cfg.get("max_retries", 3))
SOURCE = "Google Play"

print(f"🎯 Target: {PER_BANK_TARGET} reviews per bank")
print(f"📱 Apps to scrape: {len(PACKAGE_MAP)}")

# ---------------------------------------------------------
# Package Validation
# ---------------------------------------------------------
def validate_packages():
    """Validate that all package names are correct"""
    print("\n🔍 Validating package names...")
    valid_packages = {}
    
    for pkg, bank_name in PACKAGE_MAP.items():
        try:
            app_info = app(pkg)
            print(f"✅ {bank_name}: {pkg} → '{app_info['title']}'")
            valid_packages[pkg] = bank_name
        except Exception as e:
            print(f"❌ {bank_name}: {pkg} → Error: {e}")
    
    return valid_packages

# ---------------------------------------------------------
# Scrape function
# ---------------------------------------------------------
def fetch_reviews_for_app(package_name, bank_name, target=PER_BANK_TARGET):
    print(f"🔍 Starting scrape for {bank_name} ({package_name})...")
    all_reviews = []
    token = None
    retries = 0

    pbar = tqdm(total=target, desc=f"Scraping {bank_name}", unit="rev")

    while len(all_reviews) < target:
        try:
            results, token = reviews(
                package_name,
                lang="en",
                country="et",  # Changed to Ethiopia
                sort=Sort.NEWEST,
                count=100,
                continuation_token=token
            )
        except Exception as e:
            retries += 1
            print(f"[{bank_name}] fetch error: {e} (retry {retries}/{MAX_RETRIES})")

            if retries >= MAX_RETRIES:
                print(f"❌ Max retries reached for {bank_name}. Moving to next bank.")
                break

            time.sleep(SLEEP * 2)
            continue

        retries = 0

        if not results:
            print(f"ℹ️  No more results for {bank_name}")
            break

        for r in results:
            row = {
                "review_id": r.get("reviewId", ""),
                "review": r.get("content", ""),
                "score": r.get("score", ""),
                "at": r.get("at").isoformat() if r.get("at") else "",
                "user_name": r.get("userName", ""),
                "reply_text": r.get("replyContent") or "",
                "reply_date": r.get("repliedAt").isoformat() if r.get("repliedAt") else "",
                "package_name": package_name,
                "bank_name": bank_name,
                "source": SOURCE
            }
            all_reviews.append(row)
            pbar.update(1)

            if len(all_reviews) >= target:
                break

        if not token:
            print(f"ℹ️  No continuation token for {bank_name}")
            break

        time.sleep(SLEEP)

    pbar.close()
    print(f"✅ {bank_name}: Collected {len(all_reviews)} reviews")
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
    
    # Validate packages first
    valid_packages = validate_packages()
    
    if not valid_packages:
        print("❌ No valid packages found. Please check your config.yaml")
        return
    
    all_banks_reviews = []
    successful_banks = []
    failed_banks = []

    for pkg, bank_name in valid_packages.items():
        print(f"\n{'='*40}")
        print(f"📱 Processing: {bank_name}")
        print(f"{'='*40}")

        try:
            bank_reviews = fetch_reviews_for_app(pkg, bank_name, target=PER_BANK_TARGET)

            if bank_reviews:
                # Store per-bank file
                bank_file = os.path.join(PROCESSED_DIR, f"{bank_name.lower().replace(' ', '_')}_reviews.csv")
                write_csv(bank_file, bank_reviews)
                all_banks_reviews.extend(bank_reviews)
                successful_banks.append(bank_name)
            else:
                failed_banks.append(bank_name)
                print(f"❌ No reviews collected for {bank_name}")
            
        except Exception as e:
            print(f"❌ Error processing {bank_name}: {e}")
            failed_banks.append(bank_name)
            continue

    # Write combined dataset
    if all_banks_reviews:
        write_csv(ALL_RAW_PATH, all_banks_reviews)
        print(f"\n🎉 Scraping completed!")
        print(f"📊 Total reviews collected: {len(all_banks_reviews)}")
        print(f"✅ Successful banks: {', '.join(successful_banks)}")
        if failed_banks:
            print(f"❌ Failed banks: {', '.join(failed_banks)}")
        print(f"📁 Output directory: {PROCESSED_DIR}")
    else:
        print("\n❌ No reviews were collected from any bank.")

if __name__ == "__main__":
    main()