"""Unit tests for Pandas cleaning, deduplication, and validation rules."""

from __future__ import annotations

import logging

import pandas as pd

from src.clean_validate import clean_and_validate


def _base_row(**overrides) -> dict:
    row = {
        "transaction_id": "TXN-1001",
        "sale_timestamp": "2026-11-01 10:15:00",
        "store_id": "st101",
        "store_name": "Mumbai Flagship",
        "store_city": "mumbai",
        "store_region": "west",
        "customer_id": "cu001",
        "customer_name": "  Aarav Sharma ",
        "customer_email": "AARAV@EXAMPLE.COM",
        "customer_segment": "loyalty gold",
        "product_id": "pr001",
        "product_name": "Headphones",
        "category": "  electronics ",
        "subcategory": "audio",
        "unit_cost": "50.00",
        "unit_price": "100.00",
        "quantity": "2",
        "discount_pct": "",
        "payment_method": "CREDIT CARD",
        "source_type": "CSV",
        "source_file": "test.csv",
    }
    row.update(overrides)
    return row


def test_clean_and_validate_standardizes_and_computes_metrics() -> None:
    logger = logging.getLogger("test_clean")
    raw_df = pd.DataFrame([
        _base_row(),
        _base_row(transaction_id="TXN-1002", sale_timestamp="01/11/2026 12:30:00", discount_pct="10"),
    ])

    result = clean_and_validate(raw_df, logger)
    assert len(result.clean_df) == 2
    assert len(result.reject_df) == 0

    first = result.clean_df.iloc[0]
    assert first["store_code"] == "ST101"
    assert first["category"] == "Electronics"
    assert first["customer_name"] == "Aarav Sharma"
    assert first["customer_email"] == "aarav@example.com"
    assert first["discount_pct"] == 0.0
    assert first["gross_amount"] == 200.0
    assert first["net_amount"] == 200.0
    assert first["profit_amount"] == 100.0

    second = result.clean_df.iloc[1]
    assert second["discount_amount"] == 20.0
    assert second["net_amount"] == 180.0
    assert second["profit_amount"] == 80.0


def test_clean_and_validate_deduplicates_and_quarantines_invalid_rows() -> None:
    logger = logging.getLogger("test_clean")
    raw_df = pd.DataFrame([
        _base_row(transaction_id="TXN-DUP", quantity="1"),
        _base_row(transaction_id="TXN-DUP", sale_timestamp="2026-11-01 11:00:00", quantity="3"),
        _base_row(transaction_id="", quantity="1"),
        _base_row(transaction_id="TXN-NEG-QTY", quantity="-2"),
        _base_row(transaction_id="TXN-BAD-PRICE", unit_price="0"),
        _base_row(transaction_id="TXN-BAD-DISC", discount_pct="95"),
        _base_row(transaction_id="TXN-BAD-DATE", sale_timestamp="not-a-date"),
    ])

    result = clean_and_validate(raw_df, logger)
    assert len(result.clean_df) == 1
    assert result.clean_df.iloc[0]["quantity"] == 3
    assert result.duplicates_removed == 1
    assert len(result.reject_df) == 5

    reasons = set(result.reject_df["rejection_reason"].tolist())
    assert "MISSING_TRANSACTION_ID" in reasons
    assert "INVALID_QUANTITY" in reasons
    assert "INVALID_UNIT_PRICE" in reasons
    assert "INVALID_DISCOUNT_PCT" in reasons
    assert "INVALID_SALE_TIMESTAMP" in reasons
