"""Pandas data cleaning, EDA profiling, validation, and quarantine splitting."""

from __future__ import annotations

import hashlib
import json
import logging
from dataclasses import dataclass
from datetime import datetime

import pandas as pd


@dataclass
class CleaningResult:
    """Outputs of the cleaning and validation stage."""

    clean_df: pd.DataFrame
    reject_df: pd.DataFrame
    eda_summary: dict
    duplicates_removed: int


def _clean_str(val: object) -> str:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return ""
    return " ".join(str(val).strip().split())


def _parse_timestamp(raw_ts: str) -> datetime | None:
    text = _clean_str(raw_ts)
    if not text:
        return None
    for fmt in (
        "%Y-%m-%d %H:%M:%S",
        "%d/%m/%Y %H:%M:%S",
        "%Y/%m/%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d",
        "%d-%m-%Y %H:%M:%S",
    ):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    parsed = pd.to_datetime(text, errors="coerce")
    if pd.isna(parsed):
        return None
    return parsed.to_pydatetime()


def _to_float(val: object) -> float | None:
    text = _clean_str(val)
    if text == "":
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _compute_record_hash(row: dict) -> str:
    hash_fields = {
        "transaction_id": row["transaction_id"],
        "sale_timestamp": row["sale_timestamp"].strftime("%Y-%m-%d %H:%M:%S"),
        "store_code": row["store_code"],
        "customer_code": row["customer_code"],
        "product_code": row["product_code"],
        "quantity": int(row["quantity"]),
        "unit_cost": f"{row['unit_cost']:.2f}",
        "unit_price": f"{row['unit_price']:.2f}",
        "discount_pct": f"{row['discount_pct']:.2f}",
        "net_amount": f"{row['net_amount']:.2f}",
        "payment_method": row["payment_method"],
    }
    encoded = json.dumps(hash_fields, sort_keys=True).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def compute_eda_summary(raw_df: pd.DataFrame) -> dict:
    """Compute Pandas Exploratory Data Analysis (EDA) quality metrics on raw data."""
    if raw_df.empty:
        return {
            "total_raw_rows": 0,
            "distinct_transaction_ids": 0,
            "duplicate_transaction_ids": 0,
            "blank_counts": {},
            "source_breakdown": {},
        }

    blank_counts = {
        col: int((raw_df[col].astype(str).str.strip() == "").sum())
        for col in raw_df.columns
        if col not in ("source_type", "source_file")
    }
    non_blank_txns = raw_df["transaction_id"].astype(str).str.strip()
    non_blank_txns = non_blank_txns[non_blank_txns != ""]
    dup_txns = int(non_blank_txns.duplicated().sum())

    return {
        "total_raw_rows": int(len(raw_df)),
        "distinct_transaction_ids": int(non_blank_txns.nunique()),
        "duplicate_transaction_ids": dup_txns,
        "blank_counts": {k: v for k, v in blank_counts.items() if v > 0},
        "source_breakdown": raw_df["source_type"].value_counts().to_dict(),
    }


def clean_and_validate(raw_df: pd.DataFrame, logger: logging.Logger) -> CleaningResult:
    """Standardize, deduplicate, and validate raw records against business rules."""
    eda_summary = compute_eda_summary(raw_df)
    if raw_df.empty:
        return CleaningResult(
            clean_df=pd.DataFrame(),
            reject_df=pd.DataFrame(),
            eda_summary=eda_summary,
            duplicates_removed=0,
        )

    valid_rows: list[dict] = []
    reject_rows: list[dict] = []

    for record in raw_df.to_dict(orient="records"):
        reasons: list[str] = []

        txn_id = _clean_str(record.get("transaction_id")).upper()
        store_code = _clean_str(record.get("store_id")).upper()
        product_code = _clean_str(record.get("product_id")).upper()
        customer_code = _clean_str(record.get("customer_id")).upper()

        if not txn_id:
            reasons.append("MISSING_TRANSACTION_ID")
        if not store_code:
            reasons.append("MISSING_STORE_ID")
        if not product_code:
            reasons.append("MISSING_PRODUCT_ID")
        if not customer_code:
            reasons.append("MISSING_CUSTOMER_ID")

        sale_ts = _parse_timestamp(str(record.get("sale_timestamp", "")))
        if sale_ts is None:
            reasons.append("INVALID_SALE_TIMESTAMP")

        unit_cost = _to_float(record.get("unit_cost"))
        unit_price = _to_float(record.get("unit_price"))
        quantity_f = _to_float(record.get("quantity"))

        discount_raw = _clean_str(record.get("discount_pct"))
        discount_pct = 0.0 if discount_raw == "" else _to_float(discount_raw)

        if quantity_f is None or quantity_f <= 0 or not float(quantity_f).is_integer():
            reasons.append("INVALID_QUANTITY")
        if unit_price is None or unit_price <= 0:
            reasons.append("INVALID_UNIT_PRICE")
        if unit_cost is None or unit_cost < 0:
            reasons.append("INVALID_UNIT_COST")
        if discount_pct is None or discount_pct < 0 or discount_pct > 80:
            reasons.append("INVALID_DISCOUNT_PCT")

        if reasons:
            reject_rows.append(
                {
                    "source_type": _clean_str(record.get("source_type")) or "UNKNOWN",
                    "source_file": _clean_str(record.get("source_file")) or "UNKNOWN",
                    "transaction_id": txn_id or None,
                    "raw_payload": json.dumps(record, default=str),
                    "rejection_reason": "; ".join(reasons),
                }
            )
            continue

        assert sale_ts is not None
        assert unit_cost is not None and unit_price is not None
        assert quantity_f is not None and discount_pct is not None

        qty = int(quantity_f)
        gross_amt = round(unit_price * qty, 2)
        disc_amt = round(gross_amt * (discount_pct / 100.0), 2)
        net_amt = round(gross_amt - disc_amt, 2)
        cost_amt = round(unit_cost * qty, 2)
        profit_amt = round(net_amt - cost_amt, 2)

        cust_name = _clean_str(record.get("customer_name")) or "Walk-in Customer"
        cust_email = _clean_str(record.get("customer_email")).lower() or None
        cust_segment = _clean_str(record.get("customer_segment")).title() or "Regular"

        clean_record = {
            "transaction_id": txn_id,
            "sale_date": sale_ts.date(),
            "sale_timestamp": sale_ts,
            "store_code": store_code,
            "store_name": _clean_str(record.get("store_name")) or f"Store {store_code}",
            "store_city": _clean_str(record.get("store_city")).title() or "Unknown",
            "store_region": _clean_str(record.get("store_region")).title() or "Unknown",
            "customer_code": customer_code,
            "customer_name": cust_name,
            "customer_email": cust_email,
            "customer_segment": cust_segment,
            "product_code": product_code,
            "product_name": _clean_str(record.get("product_name")) or f"Product {product_code}",
            "category": _clean_str(record.get("category")).title() or "Uncategorized",
            "subcategory": _clean_str(record.get("subcategory")).title() or "General",
            "unit_cost": round(unit_cost, 2),
            "unit_price": round(unit_price, 2),
            "quantity": qty,
            "discount_pct": round(discount_pct, 2),
            "gross_amount": gross_amt,
            "discount_amount": disc_amt,
            "net_amount": net_amt,
            "cost_amount": cost_amt,
            "profit_amount": profit_amt,
            "payment_method": _clean_str(record.get("payment_method")).title() or "Unknown",
            "source_type": _clean_str(record.get("source_type")) or "CSV",
            "source_file": _clean_str(record.get("source_file")) or "UNKNOWN",
        }
        clean_record["record_hash"] = _compute_record_hash(clean_record)
        valid_rows.append(clean_record)

    clean_df = pd.DataFrame(valid_rows)
    duplicates_removed = 0
    if not clean_df.empty:
        before_dedup = len(clean_df)
        clean_df = clean_df.sort_values("sale_timestamp").drop_duplicates(
            subset=["transaction_id"], keep="last"
        ).reset_index(drop=True)
        duplicates_removed = before_dedup - len(clean_df)

    reject_df = pd.DataFrame(reject_rows)
    logger.info(
        "Cleaning complete | raw=%d | clean=%d | duplicates_removed=%d | rejected=%d",
        len(raw_df),
        len(clean_df),
        duplicates_removed,
        len(reject_df),
    )

    return CleaningResult(
        clean_df=clean_df,
        reject_df=reject_df,
        eda_summary=eda_summary,
        duplicates_removed=duplicates_removed,
    )
