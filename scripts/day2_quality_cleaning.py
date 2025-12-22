# scripts/run_day2_clean.py
import pandas as pd
from pathlib import Path
import logging
from bootcamp_data.config import make_paths
from bootcamp_data.io import read_orders_csv, write_parquet
from bootcamp_data.transforms import enforce_schema, missingness_report, add_missing_flags, normalize_text
from bootcamp_data.quality import require_columns, assert_non_empty, assert_unique_key, assert_in_range

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
log = logging.getLogger(__name__)

def main():
    ROOT = Path(__file__).resolve().parents[1]
    p = make_paths(ROOT)

    orders_raw = enforce_schema(read_orders_csv(p.raw / "orders.csv"))
    orders = orders_raw.copy()

    NA = ["", "NA", "N/A", "null", "None"]
    users = pd.read_csv(
        p.raw / "users.csv",
        dtype={"user_id": "string"},
        na_values=NA,
        keep_default_na=True
    )

    log.info("Loaded rows: orders=%s users=%s", len(orders), len(users))

    # ===== Quality Checks =====
    require_columns(orders_raw, ["order_id", "user_id", "amount"])
    assert_non_empty(orders_raw, "orders")
    assert_unique_key(users, "user_id")
    try:
        assert_unique_key(orders_raw, "order_id")
    except AssertionError as e:
        log.info("Correctly caught duplicates: %s", e)

    try:
        assert_in_range(orders_raw["amount"], lo=0, name="amount")
    except AssertionError as e:
        log.info("Correctly caught negative amount: %s", e)

    # ===== Missingness report =====
    report = missingness_report(orders)
    report_path = p.processed / "missingness_orders_missingness.csv"
    report.to_csv(report_path)
    log.info("Missingness report saved to: %s", report_path)

    # ===== Clean columns =====
    orders["status_clean"] = normalize_text(orders["status"])
    orders = add_missing_flags(orders, ["amount", "quantity"])

    # ===== Save processed orders =====
    out_orders_clean = p.processed / "orders_clean.parquet"
    write_parquet(orders, out_orders_clean)
    log.info("Cleaned orders saved to: %s", out_orders_clean)

if __name__ == "__main__":
    main()