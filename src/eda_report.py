"""Generate automated Pandas EDA and Data Quality summary reports."""

from __future__ import annotations

from pathlib import Path

import pandas as pd


def write_eda_report(
    reports_dir: Path,
    run_id: int,
    eda_summary: dict,
    clean_df: pd.DataFrame,
    reject_df: pd.DataFrame,
    duplicates_removed: int,
) -> Path:
    """Write a markdown EDA & Data Quality report for a pipeline run."""
    reports_dir.mkdir(parents=True, exist_ok=True)
    report_path = reports_dir / f"eda_run_{run_id}.md"

    lines: list[str] = [
        f"# Retail Sales EDA & Data Quality Report (Run #{run_id})",
        "",
        "## 1. Ingestion & Data Quality Summary",
        f"- **Total Raw Rows Extracted**: {eda_summary.get('total_raw_rows', 0)}",
        f"- **Distinct Transaction IDs**: {eda_summary.get('distinct_transaction_ids', 0)}",
        f"- **Duplicate Rows Removed**: {duplicates_removed}",
        f"- **Valid Clean Rows**: {len(clean_df)}",
        f"- **Quarantined / Rejected Rows**: {len(reject_df)}",
        "",
        "### Source Breakdown",
    ]
    for src, cnt in eda_summary.get("source_breakdown", {}).items():
        lines.append(f"- `{src}`: {cnt} rows")

    blank_counts = eda_summary.get("blank_counts", {})
    lines.extend(["", "### Missing / Blank Raw Fields Detected"])
    if blank_counts:
        for col, cnt in blank_counts.items():
            lines.append(f"- `{col}`: {cnt} blanks")
    else:
        lines.append("- None")

    lines.extend(["", "### Rejection Reasons Breakdown"])
    if not reject_df.empty:
        for reason, cnt in reject_df["rejection_reason"].value_counts().items():
            lines.append(f"- `{reason}`: {cnt} rows")
    else:
        lines.append("- Zero rejected records")

    if not clean_df.empty:
        lines.extend([
            "",
            "## 2. Cleaned Dataset Financial & Store Profile",
            f"- **Total Gross Revenue**: {clean_df['gross_amount'].sum():,.2f}",
            f"- **Total Discounts**: {clean_df['discount_amount'].sum():,.2f}",
            f"- **Total Net Revenue**: {clean_df['net_amount'].sum():,.2f}",
            f"- **Total Profit**: {clean_df['profit_amount'].sum():,.2f}",
            "",
            "### Revenue by Category",
            "| Category | Transactions | Units Sold | Net Revenue | Profit |",
            "|---|---:|---:|---:|---:|",
        ])
        cat_agg = (
            clean_df.groupby("category", as_index=False)
            .agg(
                txns=("transaction_id", "count"),
                units=("quantity", "sum"),
                net_rev=("net_amount", "sum"),
                profit=("profit_amount", "sum"),
            )
            .sort_values("net_rev", ascending=False)
        )
        for _, row in cat_agg.iterrows():
            lines.append(
                f"| {row['category']} | {int(row['txns'])} | {int(row['units'])} | "
                f"{row['net_rev']:,.2f} | {row['profit']:,.2f} |"
            )

    report_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return report_path
