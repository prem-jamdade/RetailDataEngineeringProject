"""Generate realistic multi-store daily CSVs and Sales API JSON payloads.

Scales to 100,000+ retail transactions across 10 physical stores + 1 online
channel over 30 days, with deliberate data quality anomalies (whitespace,
mixed casing, mixed timestamp formats, duplicates, missing discounts,
negative quantities, invalid prices, excessive discounts, and missing IDs)
plus amended records to demonstrate incremental MERGE upserts.
"""

from __future__ import annotations

import argparse
import json
import random
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

from config.settings import get_pipeline_config

STORES = [
    {"store_id": "ST101", "store_name": "Mumbai Flagship Store", "store_city": "Mumbai", "store_region": "West"},
    {"store_id": "ST102", "store_name": "Bengaluru TechHub", "store_city": "Bengaluru", "store_region": "South"},
    {"store_id": "ST103", "store_name": "Delhi Metro Mall", "store_city": "New Delhi", "store_region": "North"},
    {"store_id": "ST104", "store_name": "Pune Highstreet", "store_city": "Pune", "store_region": "West"},
    {"store_id": "ST105", "store_name": "Hyderabad CyberPlaza", "store_city": "Hyderabad", "store_region": "South"},
    {"store_id": "ST106", "store_name": "Chennai Marina Center", "store_city": "Chennai", "store_region": "South"},
    {"store_id": "ST107", "store_name": "Kolkata Park Galleria", "store_city": "Kolkata", "store_region": "East"},
    {"store_id": "ST108", "store_name": "Ahmedabad Riverfront", "store_city": "Ahmedabad", "store_region": "West"},
    {"store_id": "ST109", "store_name": "Jaipur Heritage Square", "store_city": "Jaipur", "store_region": "North"},
    {"store_id": "ST110", "store_name": "Chandigarh Central", "store_city": "Chandigarh", "store_region": "North"},
]

PRODUCTS = [
    {"product_id": "PR001", "product_name": "Wireless Noise-Canceling Headphones", "category": "Electronics", "subcategory": "Audio", "unit_cost": 85.00, "unit_price": 149.99},
    {"product_id": "PR002", "product_name": "4K Ultra-HD Monitor 27-inch", "category": "Electronics", "subcategory": "Displays", "unit_cost": 210.00, "unit_price": 329.00},
    {"product_id": "PR003", "product_name": "Mechanical Gaming Keyboard", "category": "Electronics", "subcategory": "Peripherals", "unit_cost": 45.00, "unit_price": 89.50},
    {"product_id": "PR004", "product_name": "Ergonomic Office Chair", "category": "Furniture", "subcategory": "Seating", "unit_cost": 120.00, "unit_price": 249.00},
    {"product_id": "PR005", "product_name": "Standing Desk Converter", "category": "Furniture", "subcategory": "Desks", "unit_cost": 75.00, "unit_price": 159.00},
    {"product_id": "PR006", "product_name": "Cotton Crewneck T-Shirt", "category": "Apparel", "subcategory": "Tops", "unit_cost": 8.50, "unit_price": 24.99},
    {"product_id": "PR007", "product_name": "Running Sneakers Pro", "category": "Apparel", "subcategory": "Footwear", "unit_cost": 38.00, "unit_price": 95.00},
    {"product_id": "PR008", "product_name": "Stainless Steel Espresso Maker", "category": "Home & Kitchen", "subcategory": "Appliances", "unit_cost": 65.00, "unit_price": 139.00},
    {"product_id": "PR009", "product_name": "Insulated Travel Tumbler", "category": "Home & Kitchen", "subcategory": "Drinkware", "unit_cost": 9.00, "unit_price": 26.00},
    {"product_id": "PR010", "product_name": "USB-C Multiport Docking Hub", "category": "Electronics", "subcategory": "Accessories", "unit_cost": 22.00, "unit_price": 54.99},
    {"product_id": "PR011", "product_name": "Smartwatch Fitness Tracker", "category": "Electronics", "subcategory": "Wearables", "unit_cost": 72.00, "unit_price": 139.99},
    {"product_id": "PR012", "product_name": "Bluetooth Portable Speaker", "category": "Electronics", "subcategory": "Audio", "unit_cost": 32.00, "unit_price": 69.99},
    {"product_id": "PR013", "product_name": "LED Desk Lamp Pro", "category": "Furniture", "subcategory": "Lighting", "unit_cost": 18.00, "unit_price": 44.00},
    {"product_id": "PR014", "product_name": "Modular Bookshelf 5-Tier", "category": "Furniture", "subcategory": "Storage", "unit_cost": 68.00, "unit_price": 145.00},
    {"product_id": "PR015", "product_name": "Denim Slim-Fit Jeans", "category": "Apparel", "subcategory": "Bottoms", "unit_cost": 21.00, "unit_price": 59.99},
    {"product_id": "PR016", "product_name": "Waterproof Windbreaker Jacket", "category": "Apparel", "subcategory": "Outerwear", "unit_cost": 34.00, "unit_price": 84.99},
    {"product_id": "PR017", "product_name": "Air Fryer Digital 5L", "category": "Home & Kitchen", "subcategory": "Appliances", "unit_cost": 52.00, "unit_price": 119.00},
    {"product_id": "PR018", "product_name": "Non-Stick Cookware Set 6pc", "category": "Home & Kitchen", "subcategory": "Cookware", "unit_cost": 48.00, "unit_price": 115.00},
    {"product_id": "PR019", "product_name": "Yoga Mat Anti-Slip 8mm", "category": "Sports & Fitness", "subcategory": "Equipment", "unit_cost": 11.00, "unit_price": 32.00},
    {"product_id": "PR020", "product_name": "Adjustable Dumbbell Pair 20kg", "category": "Sports & Fitness", "subcategory": "Strength", "unit_cost": 62.00, "unit_price": 134.00},
    {"product_id": "PR021", "product_name": "Laptop Backpack Water-Resistant", "category": "Accessories", "subcategory": "Bags", "unit_cost": 19.00, "unit_price": 49.99},
    {"product_id": "PR022", "product_name": "Wireless Vertical Ergonomic Mouse", "category": "Electronics", "subcategory": "Peripherals", "unit_cost": 16.00, "unit_price": 39.99},
    {"product_id": "PR023", "product_name": "Ceramic Pour-Over Coffee Set", "category": "Home & Kitchen", "subcategory": "Drinkware", "unit_cost": 14.00, "unit_price": 38.00},
    {"product_id": "PR024", "product_name": "Polarized Aviator Sunglasses", "category": "Accessories", "subcategory": "Eyewear", "unit_cost": 15.00, "unit_price": 45.00},
    {"product_id": "PR025", "product_name": "Memory Foam Lumbar Cushion", "category": "Furniture", "subcategory": "Accessories", "unit_cost": 12.00, "unit_price": 34.99},
]

_BASE_CUSTOMERS = [
    ("Aarav Sharma", "Loyalty Gold"),
    ("Priya Nair", "Corporate"),
    ("Rohan Verma", "Regular"),
    ("Sneha Kulkarni", "Loyalty Platinum"),
    ("Vikram Mehta", "Regular"),
    ("Ananya Iyer", "Loyalty Gold"),
    ("Karan Malhotra", "Corporate"),
    ("Divya Reddy", "Regular"),
    ("Siddharth Joshi", "Loyalty Gold"),
    ("Meera Deshmukh", "Loyalty Platinum"),
    ("Arjun Kapoor", "Regular"),
    ("Neha Chatterjee", "Corporate"),
    ("Rahul Menon", "Regular"),
    ("Pooja Patel", "Loyalty Gold"),
    ("Aditya Rao", "Regular"),
]


def _build_customer_pool(total: int = 120) -> list[dict]:
    pool: list[dict] = []
    for idx in range(1, total + 1):
        base_name, seg = _BASE_CUSTOMERS[(idx - 1) % len(_BASE_CUSTOMERS)]
        suffix = "" if idx <= len(_BASE_CUSTOMERS) else f" {idx}"
        full_name = f"{base_name}{suffix}"
        email_slug = full_name.lower().replace(" ", ".")
        pool.append({
            "customer_id": f"CU{idx:03d}",
            "customer_name": full_name,
            "customer_email": None if idx % 10 == 0 else f"{email_slug}@example.com",
            "customer_segment": seg,
        })
    return pool


CUSTOMERS = _build_customer_pool(120)
PAYMENT_METHODS = ["Credit Card", "UPI", "Debit Card", "Cash", "Net Banking"]


def _build_valid_row(
    rng: random.Random,
    txn_id: str,
    sale_dt: datetime,
    store: dict,
) -> dict:
    product = rng.choice(PRODUCTS)
    customer = rng.choice(CUSTOMERS)
    qty = rng.randint(1, 5)
    discount = rng.choice([0, 0, 0, 5, 10, 15, 20])
    return {
        "transaction_id": txn_id,
        "sale_timestamp": sale_dt.strftime("%Y-%m-%d %H:%M:%S"),
        "store_id": store["store_id"],
        "store_name": store["store_name"],
        "store_city": store["store_city"],
        "store_region": store["store_region"],
        "customer_id": customer["customer_id"],
        "customer_name": customer["customer_name"],
        "customer_email": customer["customer_email"] or "",
        "customer_segment": customer["customer_segment"],
        "product_id": product["product_id"],
        "product_name": product["product_name"],
        "category": product["category"],
        "subcategory": product["subcategory"],
        "unit_cost": product["unit_cost"],
        "unit_price": product["unit_price"],
        "quantity": qty,
        "discount_pct": discount,
        "payment_method": rng.choice(PAYMENT_METHODS),
    }


def generate_batch_datasets(
    raw_dir: Path | None = None,
    api_dir: Path | None = None,
    days: list[str] | None = None,
    num_days: int = 30,
    rows_per_store_day: int = 300,
    rows_per_api_day: int = 500,
    clean_existing: bool = False,
    seed: int = 42,
) -> dict[str, list[Path]]:
    """Create multi-store CSV drops and API JSON files for specified dates."""
    cfg = get_pipeline_config()
    raw_dir = raw_dir or cfg.raw_dir
    api_dir = api_dir or cfg.api_feed_dir
    raw_dir.mkdir(parents=True, exist_ok=True)
    api_dir.mkdir(parents=True, exist_ok=True)

    if clean_existing:
        for old_csv in raw_dir.glob("*.csv"):
            old_csv.unlink()
        for old_json in api_dir.glob("*.json"):
            old_json.unlink()

    if days is None:
        end_dt = datetime(2026, 11, 3)
        start_dt = end_dt - timedelta(days=num_days - 1)
        days = [(start_dt + timedelta(days=offset)).strftime("%Y-%m-%d") for offset in range(num_days)]

    rng = random.Random(seed)
    created_csvs: list[Path] = []
    created_jsons: list[Path] = []

    txn_counter = 100000
    first_day_sample_for_update: dict | None = None

    for day_idx, day_str in enumerate(days):
        base_dt = datetime.strptime(day_str, "%Y-%m-%d")
        for store in STORES:
            rows: list[dict] = []
            for i in range(rows_per_store_day):
                txn_counter += 1
                sale_dt = base_dt + timedelta(
                    hours=rng.randint(9, 21),
                    minutes=rng.randint(0, 59),
                    seconds=rng.randint(0, 59),
                )
                row = _build_valid_row(
                    rng,
                    f"TXN-{day_str.replace('-', '')}-{store['store_id']}-{txn_counter}",
                    sale_dt,
                    store,
                )

                # Inject fixable formatting anomalies (~4% of rows)
                if i % 50 == 2:
                    row["category"] = f"  {row['category'].lower()}  "
                    row["payment_method"] = row["payment_method"].upper()
                    row["discount_pct"] = ""  # missing discount -> defaults to 0
                elif i % 50 == 15:
                    row["sale_timestamp"] = sale_dt.strftime("%d/%m/%Y %H:%M:%S")
                    row["customer_name"] = f"  {row['customer_name']} "
                elif i == 8 and first_day_sample_for_update is None and day_idx == 0:
                    first_day_sample_for_update = dict(row)

                rows.append(row)

            # Inject periodic duplicate row (~1 per 2 store-days)
            if day_idx % 2 == 0 and store["store_id"] in ("ST101", "ST105"):
                rows.append(dict(rows[0]))

            # Inject ~1% realistic invalid rows to exercise quarantine pipeline
            if store["store_id"] in ("ST102", "ST106"):
                bad_qty = dict(rows[1])
                bad_qty["transaction_id"] = f"TXN-BAD-QTY-{day_idx}-{store['store_id']}"
                bad_qty["quantity"] = -rng.randint(1, 5)
                rows.append(bad_qty)

            if store["store_id"] in ("ST103", "ST107"):
                bad_price = dict(rows[3])
                bad_price["transaction_id"] = f"TXN-BAD-PRICE-{day_idx}-{store['store_id']}"
                bad_price["unit_price"] = 0
                rows.append(bad_price)

                bad_disc = dict(rows[4])
                bad_disc["transaction_id"] = f"TXN-BAD-DISC-{day_idx}-{store['store_id']}"
                bad_disc["discount_pct"] = 95
                rows.append(bad_disc)

            if store["store_id"] in ("ST104", "ST108"):
                missing_id = dict(rows[0])
                missing_id["transaction_id"] = ""
                rows.append(missing_id)

            if day_idx == 1 and store["store_id"] == "ST101" and first_day_sample_for_update is not None:
                amended = dict(first_day_sample_for_update)
                amended["quantity"] = int(amended["quantity"]) + 2
                amended["discount_pct"] = 10
                rows.append(amended)

            csv_path = raw_dir / f"{store['store_id'].lower()}_{day_str}.csv"
            pd.DataFrame(rows).to_csv(csv_path, index=False)
            created_csvs.append(csv_path)

        # Online Sales API JSON payload for each day
        online_store = {
            "store_id": "ONL01",
            "store_name": "E-Commerce Web Store",
            "store_city": "National",
            "store_region": "Online",
        }
        api_rows = []
        for j in range(rows_per_api_day):
            txn_counter += 1
            sale_dt = base_dt + timedelta(
                hours=rng.randint(0, 23),
                minutes=rng.randint(0, 59),
                seconds=rng.randint(0, 59),
            )
            row = _build_valid_row(
                rng,
                f"API-{day_str.replace('-', '')}-{txn_counter}",
                sale_dt,
                online_store,
            )
            if j % 40 == 3:
                row["customer_id"] = "WALKIN"
                row["customer_name"] = ""
            api_rows.append(row)

        if day_idx % 3 == 0:
            api_rows.append(dict(api_rows[0]))  # Duplicate API transaction
            bad_ts = dict(api_rows[1])
            bad_ts["transaction_id"] = f"API-BAD-TS-{day_idx}"
            bad_ts["sale_timestamp"] = "INVALID_DATE"
            api_rows.append(bad_ts)

        json_path = api_dir / f"online_sales_{day_str}.json"
        json_path.write_text(json.dumps({"date": day_str, "records": api_rows}), encoding="utf-8")
        created_jsons.append(json_path)

    return {"csv_files": created_csvs, "api_files": created_jsons}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate large-scale retail CSV and API JSON datasets.")
    parser.add_argument("--days", type=int, default=30, help="Number of daily batches to generate (default: 30)")
    parser.add_argument("--rows-per-store", type=int, default=300, help="Rows per store per day (default: 300)")
    parser.add_argument("--rows-per-api", type=int, default=500, help="Online API rows per day (default: 500)")
    parser.add_argument("--no-clean", action="store_true", help="Do not delete existing raw/api files first")
    args = parser.parse_args()

    result = generate_batch_datasets(
        num_days=args.days,
        rows_per_store_day=args.rows_per_store,
        rows_per_api_day=args.rows_per_api,
        clean_existing=not args.no_clean,
    )
    print(f"Generated {len(result['csv_files'])} CSV files and {len(result['api_files'])} API JSON files.")
