"""
Download several months of NYC Yellow Taxi trip data (Parquet) from the TLC.

Usage:
    python download_data.py                          # Jan-Jun 2025 (6 months) into ./data
    python download_data.py --start 2025-01 --months 4
    python download_data.py --start 2025-01 --months 11 --out data

Notes:
  * Use consecutive months of the SAME year: Spark cannot read in one go files
    whose column types differ (e.g. passenger_count changed type between years).
  * Files already downloaded are skipped, so it is safe to re-run.
"""
import argparse
import os
import urllib.error
import urllib.request

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{ym}.parquet"


def month_range(start, n):
    year, month = map(int, start.split("-"))
    for _ in range(n):
        yield f"{year}-{month:02d}"
        month += 1
        if month == 13:
            year, month = year + 1, 1


def download(url, dest):
    tmp = dest + ".part"

    def progress(blocks, block_size, total):
        if total > 0:
            pct = min(100.0, blocks * block_size * 100 / total)
            print(f"\r   {pct:5.1f}%", end="", flush=True)

    urllib.request.urlretrieve(url, tmp, progress)
    os.replace(tmp, dest)
    print()


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--start", default="2025-01", help="first month, format YYYY-MM")
    p.add_argument("--months", type=int, default=6, help="number of consecutive months")
    p.add_argument("--out", default="data", help="output folder")
    args = p.parse_args()

    os.makedirs(args.out, exist_ok=True)
    for ym in month_range(args.start, args.months):
        dest = os.path.join(args.out, f"yellow_tripdata_{ym}.parquet")
        if os.path.exists(dest):
            print(f"{ym}: already downloaded, skipping")
            continue
        print(f"{ym}: downloading...")
        try:
            download(BASE_URL.format(ym=ym), dest)
        except urllib.error.HTTPError as e:
            print(f"\n   Could not download {ym} (HTTP {e.code}). "
                  f"The month may not be published yet.")
            if os.path.exists(dest + ".part"):
                os.remove(dest + ".part")

    files = sorted(f for f in os.listdir(args.out) if f.endswith(".parquet"))
    total_mb = sum(os.path.getsize(os.path.join(args.out, f)) for f in files) / 1e6
    print(f"\nFolder '{args.out}': {len(files)} files, {total_mb:.1f} MB in total")
    for f in files:
        print("  ", f)


if __name__ == "__main__":
    main()