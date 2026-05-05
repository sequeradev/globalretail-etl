"""Compute the 8 business insights used in the presentation.

Reads the source CSV directly so the numbers can be reproduced from scratch
without having to spin up the full Airflow + MongoDB stack. Output is
written as Markdown to ``docs/business_insights.md``.

Run from the project root:
    python scripts/compute_insights.py
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd


CSV_PATH = Path(__file__).resolve().parents[1] / "include" / "data" / "online_retail.csv"
OUT_PATH = Path(__file__).resolve().parents[1] / "docs" / "business_insights.md"
FX_RATE = 1.17  # GBP -> EUR approximate average over 2010-2011


def load() -> pd.DataFrame:
    df = pd.read_csv(CSV_PATH, encoding="ISO-8859-1", parse_dates=["InvoiceDate"])
    df = df.dropna(subset=["CustomerID"])
    df = df[(df["Quantity"] > 0) & (df["UnitPrice"] > 0)]
    df["revenue_gbp"] = df["Quantity"] * df["UnitPrice"]
    df["revenue_eur"] = df["revenue_gbp"] * FX_RATE
    df["month"] = df["InvoiceDate"].dt.to_period("M")
    return df


def fmt_eur(v: float) -> str:
    return f"€{v:,.0f}"


def main() -> None:
    df = load()

    total_rev = df["revenue_eur"].sum()
    n_customers = df["CustomerID"].nunique()
    n_orders = df["InvoiceNo"].nunique()
    avg_order = total_rev / n_orders

    by_country = (
        df.groupby("Country")["revenue_eur"]
        .sum()
        .sort_values(ascending=False)
    )
    uk_pct = 100 * by_country["United Kingdom"] / total_rev
    top5_non_uk = by_country.drop("United Kingdom").head(5)

    by_month = df.groupby("month")["revenue_eur"].sum().sort_index()
    peak_month = by_month.idxmax()
    peak_revenue = by_month.max()
    avg_month = by_month.median()
    peak_vs_median = peak_revenue / avg_month

    by_customer = df.groupby("CustomerID")["revenue_eur"].sum().sort_values(ascending=False)
    top10_share = 100 * by_customer.head(10).sum() / total_rev
    top1pct_n = max(1, int(len(by_customer) * 0.01))
    top1pct_share = 100 * by_customer.head(top1pct_n).sum() / total_rev

    by_product = (
        df.groupby(["StockCode", "Description"])["revenue_eur"]
        .sum()
        .sort_values(ascending=False)
        .head(10)
    )

    customer_orders = df.groupby("CustomerID")["InvoiceNo"].nunique()
    one_off_pct = 100 * (customer_orders == 1).sum() / len(customer_orders)
    repeat_pct = 100 - one_off_pct

    weekday_rev = df.groupby(df["InvoiceDate"].dt.day_name())["revenue_eur"].sum()
    weekday_rev = weekday_rev.reindex(
        ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    )

    out = []
    out.append("# Business Insights — GlobalRetail Dataset\n")
    out.append("_Computed from `online_retail.csv` after applying the same cleaning rules as the ETL pipeline._\n")
    out.append(f"**Period covered:** {df['InvoiceDate'].min().date()} → {df['InvoiceDate'].max().date()}\n")
    out.append(f"**FX rate used (illustrative):** GBP→EUR = {FX_RATE}\n\n")

    out.append("## Headline numbers\n\n")
    out.append(f"| Metric | Value |\n|---|---|\n")
    out.append(f"| Total revenue | **{fmt_eur(total_rev)}** |\n")
    out.append(f"| Unique customers | {n_customers:,} |\n")
    out.append(f"| Unique invoices | {n_orders:,} |\n")
    out.append(f"| Average order value | {fmt_eur(avg_order)} |\n")
    out.append(f"| Clean transactions | {len(df):,} |\n\n")

    out.append("## Insight 1 — Revenue is heavily UK-concentrated\n\n")
    out.append(f"The United Kingdom alone accounts for **{uk_pct:.1f}%** of total revenue ")
    out.append(f"({fmt_eur(by_country['United Kingdom'])}). The next 5 countries combined ")
    out.append(f"({', '.join(top5_non_uk.index)}) only contribute {fmt_eur(top5_non_uk.sum())} ")
    out.append(f"({100 * top5_non_uk.sum() / total_rev:.1f}%).\n\n")
    out.append("> **Implication:** any UK-specific risk (Brexit, tax change, currency shock) is ")
    out.append("an existential risk for the business. International expansion is not a growth ")
    out.append("lever — it's a survival hedge.\n\n")

    out.append("## Insight 2 — Strong seasonality with a Q4 peak\n\n")
    out.append(f"Peak month was **{peak_month}** with {fmt_eur(peak_revenue)}, ")
    out.append(f"**{peak_vs_median:.1f}× the median month** ({fmt_eur(avg_month)}).\n\n")
    out.append("Monthly breakdown (top 5):\n\n")
    top_months = by_month.sort_values(ascending=False).head(5)
    out.append("| Month | Revenue |\n|---|---|\n")
    for m, v in top_months.items():
        out.append(f"| {m} | {fmt_eur(v)} |\n")
    out.append("\n> **Implication:** inventory, staffing and marketing spend should be planned ")
    out.append("around a strong Nov-Dec peak. Smoothing demand into Q1 is a clear opportunity.\n\n")

    out.append("## Insight 3 — Revenue concentration in a few high-value customers\n\n")
    out.append(f"The top 10 customers (out of {n_customers:,}) generate **{top10_share:.1f}%** of revenue. ")
    out.append(f"The top 1% of customers ({top1pct_n:,} accounts) generate **{top1pct_share:.1f}%**.\n\n")
    out.append("> **Implication:** churn of even a single top-10 account is a measurable hit to revenue. ")
    out.append("Account-management investment for these customers has an outsized ROI.\n\n")

    out.append("## Insight 4 — Customer retention is the real growth lever\n\n")
    out.append(f"**{one_off_pct:.1f}%** of customers placed only one order in the period; ")
    out.append(f"only **{repeat_pct:.1f}%** came back. ")
    out.append("Given the revenue concentration above, converting first-time buyers into repeat ")
    out.append("buyers is mathematically more valuable than acquiring new ones.\n\n")

    out.append("## Insight 5 — Top 10 products by revenue\n\n")
    out.append("| Stock code | Description | Revenue |\n|---|---|---|\n")
    for (code, desc), v in by_product.items():
        desc_short = (desc[:50] + "…") if len(str(desc)) > 50 else desc
        out.append(f"| {code} | {desc_short} | {fmt_eur(v)} |\n")
    out.append("\n")

    out.append("## Insight 6 — Sunday is dead, midweek wins\n\n")
    out.append("| Weekday | Revenue |\n|---|---|\n")
    for day, v in weekday_rev.items():
        if pd.notna(v):
            out.append(f"| {day} | {fmt_eur(v)} |\n")
    out.append("\n> **Implication:** marketing campaigns and email sends should be timed for ")
    out.append("Tue-Thu when buying activity is highest.\n\n")

    out.append("---\n")
    out.append("_Generated by `scripts/compute_insights.py`. Re-run after each ETL update._\n")

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUT_PATH.write_text("".join(out), encoding="utf-8")
    print(f"Wrote {OUT_PATH}")


if __name__ == "__main__":
    main()
