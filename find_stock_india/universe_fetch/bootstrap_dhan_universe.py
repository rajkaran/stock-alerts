"""
One-time bootstrap: load DHAN CSV universes into MongoDB.

Reads:
  - data/dhan-all-etfs.csv     -> universe_etfs
  - data/dhan-all-stocks.csv   -> universe_stocks
  - data/dhan-all-indices.csv  -> universe_indices

We keep it simple:
  - use 'name' as the unique key (idempotent)
  - store the row as 'source_fields'
  - for ETFs, also add a 'symbol_hint' if the name contains '(TICKER)'
"""

import re
from pathlib import Path
from typing import List, Dict

import pandas as pd

from find_stock_india.common.mongo_utils import upsert_many_by_key

# Resolve repo root assuming this file lives in: stock_alerts/find_stock_india/universe_fetch/
BASE_DIR = Path(__file__).resolve().parents[2]   # .../stock_alerts
DATA_DIR = BASE_DIR / "data"


def _load_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        print(f"[WARN] CSV not found: {path}")
        return pd.DataFrame()
    df = pd.read_csv(path)
    df.columns = [c.strip() for c in df.columns]
    return df


# -------- ETFs --------

ETF_CSV = DATA_DIR / "dhan-all-etfs.csv"

def _symbol_hint_from_name(name: str) -> str | None:
    # e.g. 'Nippon Nifty 50 ETF (NIFTYBEES)' -> 'NIFTYBEES'
    if not name:
        return None
    m = re.search(r"\(([A-Z0-9._-]+)\)", name)
    return m.group(1).strip() if m else None

def load_etfs() -> None:
    df = _load_csv(ETF_CSV)
    if df.empty:
        print("[ETFs] No data to load.")
        return

    docs: List[Dict] = []
    for _, r in df.iterrows():
        name = str(r.get("Name", "")).strip()
        if not name:
            continue

        docs.append({
            "name": name,
            "source": "dhan",
            "type": "etf",
            "symbol_hint": _symbol_hint_from_name(name),
            "source_fields": r.to_dict(),   # keep whole original row
        })

    if not docs:
        print("[ETFs] No docs built.")
        return

    upserted = upsert_many_by_key("ExchangeTradedFunds", docs, key="name")
    print(f"[ETFs] {len(docs)} rows parsed; {upserted} docs upserted into universe_etfs.")


# -------- Stocks --------

STOCKS_CSV = DATA_DIR / "dhan-all-stocks.csv"

def load_stocks() -> None:
    df = _load_csv(STOCKS_CSV)
    if df.empty:
        print("[Stocks] No data to load.")
        return

    docs: List[Dict] = []
    for _, r in df.iterrows():
        name = str(r.get("Name", "")).strip()
        if not name:
            continue

        docs.append({
            "name": name,
            "source": "dhan",
            "type": "stock",
            # symbol will be derived later (e.g. from Screener slug + NSE/BSE masters)
            "symbol_hint": None,
            "screener_url": str(r.get("Screener", "")).strip(),
            "source_fields": r.to_dict(),
        })

    if not docs:
        print("[Stocks] No docs built.")
        return

    upserted = upsert_many_by_key("Stocks", docs, key="name")
    print(f"[Stocks] {len(docs)} rows parsed; {upserted} docs upserted into universe_stocks.")


# -------- Indices --------

INDICES_CSV = DATA_DIR / "dhan-all-indices.csv"

def load_indices() -> None:
    df = _load_csv(INDICES_CSV)
    if df.empty:
        print("[Indices] No data to load.")
        return

    docs: List[Dict] = []
    for _, r in df.iterrows():
        name = str(r.get("Name", "")).strip()
        if not name:
            continue

        docs.append({
            "name": name,
            "source": "dhan",
            "type": "index",
            "source_fields": r.to_dict(),
        })

    if not docs:
        print("[Indices] No docs built.")
        return

    upserted = upsert_many_by_key("IndexFunds", docs, key="name")
    print(f"[Indices] {len(docs)} rows parsed; {upserted} docs upserted into universe_indices.")


# -------- Main --------

def main() -> None:
    print(f"[INFO] Using DATA_DIR={DATA_DIR}")
    load_etfs()
    load_stocks()
    load_indices()
    print("[DONE] Bootstrap from DHAN CSVs complete.")

if __name__ == "__main__":
    main()
