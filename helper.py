import os, json, re
from datetime import datetime, timedelta, timezone
import pandas as pd
import numpy as np

def parse_tickers(env_value: str | None, default: list[str]) -> list[str]:
    if not env_value:
        return default
    s = env_value.strip()
    try:
        # Support JSON like: ["BCE.TO","BNS.TO"]
        if s.startswith('['):
            arr = json.loads(s)
            return list(dict.fromkeys([str(x).strip().upper() for x in arr if str(x).strip()]))
    except json.JSONDecodeError:
        pass
    # Fallback: comma/whitespace separated
    parts = re.split(r'[,\s]+', s)
    cleaned = [p.strip().upper() for p in parts if p.strip()]
    # De-dup, preserve order
    return list(dict.fromkeys(cleaned))

def normalize_to_utc(ts) -> datetime:
    # Accepts pandas Timestamp or datetime; returns tz-aware UTC datetime
    if isinstance(ts, pd.Timestamp):
        ts = ts.to_pydatetime()
    if ts.tzinfo is None:
        return ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(timezone.utc)

def normalize_ohlc(df: pd.DataFrame, ticker: str | None = None) -> pd.DataFrame:
    """Return Open/High/Low/Close/Volume with flat columns and UTC DatetimeIndex."""
    if df is None or df.empty:
        return pd.DataFrame()

    out = df.copy()

    # Normalize index to UTC DatetimeIndex
    if not isinstance(out.index, pd.DatetimeIndex):
        out.index = pd.to_datetime(out.index, utc=True, errors="coerce")
    elif out.index.tz is None:
        out.index = out.index.tz_localize("UTC")
    else:
        out.index = out.index.tz_convert("UTC")

    # Drop any NaT in the index (defensive)
    out = out[~out.index.isna()]

    # If MultiIndex columns, slice this ticker or flatten
    if isinstance(out.columns, pd.MultiIndex):
        if ticker:
            # Find which level actually contains the ticker symbol
            hit_level = None
            for i in range(out.columns.nlevels):
                try:
                    if ticker in out.columns.get_level_values(i):
                        hit_level = i
                        break
                except Exception:
                    pass
            if hit_level is not None:
                try:
                    # xs by the level that contains the ticker
                    out = out.xs(ticker, axis=1, level=hit_level, drop_level=True)
                except KeyError:
                    pass

        # If still MultiIndex (e.g., single-ticker slice failed), flatten columns
        if isinstance(out.columns, pd.MultiIndex):
            out.columns = [
                "_".join([str(x) for x in tup if str(x) != ""])
                for tup in out.columns.tolist()
            ]

    # Map variants to canonical names (incl. Adj Close)
    rename_map = {}
    for col in list(out.columns):
        lc = str(col).lower()
        if lc.endswith("_open") or lc == "open":        rename_map[col] = "Open"
        elif lc.endswith("_high") or lc == "high":      rename_map[col] = "High"
        elif lc.endswith("_low")  or lc == "low":       rename_map[col] = "Low"
        elif lc.endswith("_close")or lc == "close":     rename_map[col] = "Close"
        elif lc in ("adj close", "adj_close", "adjclose", "price_adj close"):
            rename_map[col] = "Adj Close"
        elif lc.endswith("_volume") or lc == "volume":  rename_map[col] = "Volume"
    if rename_map:
        out = out.rename(columns=rename_map)

    keep = [c for c in ["Open", "High", "Low", "Close", "Adj Close", "Volume"] if c in out.columns]
    out = out[keep]

    # Dedup & sort by index (stable)
    out = out[~out.index.duplicated(keep="last")].sort_index(kind="mergesort")

    return out



def scalar(x):
    """Coerce possible Series/Index/np scalar to a Python scalar."""
    if isinstance(x, (pd.Series, pd.Index)):
        if len(x) == 0:
            return float("nan")
        x = x.iloc[0]
    if isinstance(x, (np.generic,)):
        return np.asscalar(x) if hasattr(np, "asscalar") else x.item()
    return x
