# src/bootcamp_data/transforms.py
import pandas as pd
import re

_ws = re.compile(r"\s+")

# ===== Day 1: Schema Enforcement =====
def enforce_schema(df: pd.DataFrame) -> pd.DataFrame:
    """يمكن وضع أي تحويلات schema من Day1 هنا."""
    return df.assign(
        order_id=df["order_id"].astype("string"),
        user_id=df["user_id"].astype("string"),
        amount=pd.to_numeric(df["amount"], errors="coerce").astype("Float64"),
        quantity=pd.to_numeric(df["quantity"], errors="coerce").astype("Int64"),
    )

# ===== Day 2: Data Quality Helpers =====
def missingness_report(df: pd.DataFrame) -> pd.DataFrame:
    """تقرير missing لكل عمود."""
    n = len(df)
    return (
        df.isna().sum()
          .rename("n_missing")
          .to_frame()
          .assign(p_missing=lambda t: 100 * t["n_missing"] / n)
          .sort_values("p_missing", ascending=False)
    )

def add_missing_flags(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    """Add boolean columns indicating missing values (حل الدكتور)."""
    out = df.copy()
    for c in cols:
        out[f"{c}__isna"] = out[c].isna()
    return out

def normalize_text(s: pd.Series) -> pd.Series:
    """Normalize text: strip, casefold, collapse internal whitespace."""
    return (
        s.astype("string")
         .str.strip()
         .str.casefold()
         .str.replace(_ws, " ", regex=True)
    )

def apply_mapping(series: pd.Series, mapping: dict) -> pd.Series:
    """Apply value mapping, keeping unmapped values unchanged (حل الدكتور)."""
    return series.map(lambda x: mapping.get(x, x))

def dedupe_keep_latest(df: pd.DataFrame, key_cols: list[str], ts_col: str) -> pd.DataFrame:
    """Remove duplicates, keeping the latest row by timestamp (حل الدكتور)."""
    return (
        df.sort_values(ts_col)
          .drop_duplicates(subset=key_cols, keep="last")
          .reset_index(drop=True)
    )
