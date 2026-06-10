"""
vendor_drop.py - simulate an upstream vendor delivering daily CSVs (Lab 4).

Run from the HOST (not inside Docker). Uses only Python stdlib.

Examples
--------
    python scripts/vendor_drop.py --date 2026-06-01
    python scripts/vendor_drop.py --seed-pack --volume small
    python scripts/vendor_drop.py --range 2026-06-01:2026-06-14 --volume medium
    python scripts/vendor_drop.py --date 2026-06-03 --corrupt
    python scripts/vendor_drop.py --date 2026-06-04 --delay 60
    python scripts/vendor_drop.py --reference
"""
from __future__ import annotations

import argparse
import csv
import os
import random
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path


HERE = Path(__file__).resolve().parent.parent
INCOMING = HERE / "data" / "incoming"
REFERENCE = HERE / "data" / "reference"

CATEGORIES = ["electronics", "groceries", "fashion", "books", "sports", "home", "toys"]
PAYMENT_METHODS = ["card", "cash", "wallet", "transfer"]
COUNTRIES = ["FR", "DE", "ES", "IT", "BE", "NL"]

VOLUME_PRESETS = {
    "small": 200,
    "medium": 5000,
}

# 14 days used by --seed-pack (June 1--14, 2026)
SEED_PACK_START = date(2026, 6, 1)
SEED_PACK_DAYS = 14


def _parse_date(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def _seeded_rng(d: date, salt: int = 0) -> random.Random:
    return random.Random(int(d.strftime("%Y%m%d")) + salt)


def row_count_for_volume(volume: str, explicit_rows: int | None) -> int:
    if explicit_rows is not None:
        return explicit_rows
    if volume not in VOLUME_PRESETS:
        raise ValueError(f"Unknown volume {volume!r}; choose from {list(VOLUME_PRESETS)}")
    return VOLUME_PRESETS[volume]


def generate_rows(d: date, n: int = 200, corrupt: bool = False, salt: int = 0) -> list[dict]:
    rng = _seeded_rng(d, salt=salt)
    rows = []
    for i in range(n):
        amount = 0.0 if corrupt else round(rng.uniform(2.0, 250.0), 2)
        rows.append({
            "tx_id": f"{d.strftime('%Y%m%d')}-{salt:02d}-{i:05d}",
            "category": rng.choice(CATEGORIES),
            "payment_method": rng.choice(PAYMENT_METHODS),
            "country": rng.choice(COUNTRIES),
            "amount_eur": amount,
            "ts": f"{d.isoformat()}T{rng.randint(0, 23):02d}:{rng.randint(0, 59):02d}:00",
        })
    return rows


def write_csv(rows: list[dict], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fields = list(rows[0].keys())
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)
    tmp.replace(path)


def write_csv_slow(rows: list[dict], path: Path, chunks: int = 3, pause: float = 10.0) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fields = list(rows[0].keys())
    chunk_size = max(1, len(rows) // chunks)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        f.flush()
        for i in range(chunks):
            start = i * chunk_size
            end = len(rows) if i == chunks - 1 else (i + 1) * chunk_size
            writer.writerows(rows[start:end])
            f.flush()
            os.fsync(f.fileno())
            print(f"[vendor_drop --slow] chunk {i + 1}/{chunks} -> {path.stat().st_size} bytes")
            if i < chunks - 1:
                time.sleep(pause)


def write_reference_targets(path: Path | None = None) -> Path:
    """Dimension table for Track S (broadcast join): daily revenue targets per category."""
    target = path or (REFERENCE / "category_targets.csv")
    target.parent.mkdir(parents=True, exist_ok=True)
    rows = [{"category": cat, "target_revenue_eur": round(1500 + i * 350, 2)} for i, cat in enumerate(CATEGORIES)]
    write_csv(rows, target)
    print(f"[vendor_drop] wrote reference targets -> {target}")
    return target


def drop_one(
    d: date,
    *,
    corrupt: bool,
    delay: int,
    split: int,
    n: int,
    slow: bool,
) -> list[Path]:
    if delay > 0:
        print(f"[vendor_drop] waiting {delay}s before dropping {d.isoformat()}...")
        time.sleep(delay)

    if slow:
        rows = generate_rows(d, n=n, corrupt=corrupt)
        target = INCOMING / f"transactions_{d.isoformat()}.csv"
        write_csv_slow(rows, target)
        print(f"[vendor_drop] slow write finished ({len(rows)} rows) -> {target}")
        return [target]

    if split <= 1:
        rows = generate_rows(d, n=n, corrupt=corrupt)
        target = INCOMING / f"transactions_{d.isoformat()}.csv"
        write_csv(rows, target)
        print(f"[vendor_drop] wrote {len(rows):>5} rows -> {target}")
        return [target]

    paths = []
    per_file = max(1, n // split)
    for i in range(split):
        rows = generate_rows(d, n=per_file, corrupt=corrupt, salt=i + 1)
        target = INCOMING / f"transactions_{d.isoformat()}_part{i + 1}.csv"
        write_csv(rows, target)
        paths.append(target)
        print(f"[vendor_drop] wrote {len(rows):>5} rows -> {target}")
    return paths


def run_seed_pack(n: int, corrupt: bool, slow: bool, split: int) -> None:
    end = SEED_PACK_START + timedelta(days=SEED_PACK_DAYS - 1)
    print(f"[vendor_drop] seed-pack: {SEED_PACK_START} .. {end} ({SEED_PACK_DAYS} days, {n} rows/day)")
    d = SEED_PACK_START
    while d <= end:
        drop_one(d, corrupt=corrupt, delay=0, split=split, n=n, slow=slow)
        d += timedelta(days=1)


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description="Simulate vendor drops for Lab 4.")
    g = parser.add_mutually_exclusive_group(required=True)
    g.add_argument("--date", type=_parse_date, help="Single date YYYY-MM-DD")
    g.add_argument("--range", dest="rng", help="Inclusive range YYYY-MM-DD:YYYY-MM-DD")
    g.add_argument("--seed-pack", action="store_true", help=f"Drop {SEED_PACK_DAYS} days from {SEED_PACK_START}")
    g.add_argument("--reference", action="store_true", help="Write data/reference/category_targets.csv")

    parser.add_argument("--volume", choices=list(VOLUME_PRESETS), default="small",
                        help="Row count preset (default: small)")
    parser.add_argument("--rows", type=int, default=None, help="Override row count")
    parser.add_argument("--corrupt", action="store_true", help="All amounts zero")
    parser.add_argument("--delay", type=int, default=0, help="Wait N seconds before write")
    parser.add_argument("--split", type=int, default=1, help="Split into N files")
    parser.add_argument("--slow", action="store_true", help="Slow multi-chunk write")

    args = parser.parse_args(argv)
    n = row_count_for_volume(args.volume, args.rows)

    if args.reference:
        write_reference_targets()
        return 0

    if args.seed_pack:
        run_seed_pack(n, args.corrupt, args.slow, args.split)
        return 0

    if args.rng:
        try:
            a, b = args.rng.split(":")
            start, end = _parse_date(a), _parse_date(b)
        except ValueError:
            print("Range must look like YYYY-MM-DD:YYYY-MM-DD", file=sys.stderr)
            return 2
        if end < start:
            print("End date is before start date.", file=sys.stderr)
            return 2
        d = start
        while d <= end:
            drop_one(d, corrupt=args.corrupt, delay=0, split=args.split, n=n, slow=args.slow)
            d += timedelta(days=1)
    else:
        drop_one(args.date, corrupt=args.corrupt, delay=args.delay,
                 split=args.split, n=n, slow=args.slow)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
