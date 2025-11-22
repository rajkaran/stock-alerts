"""Read stocks from Mongo, fetch via yfinance, evaluate rules, rank, write CSV."""
import os
import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta
from common.mongo_utils import col


OUT = os.getenv("OUT_STOCKS", "output/stock_results.csv")
LOOKBACK_YEARS = int(os.getenv("LOOKBACK_YEARS", 5))
MIN_LISTED_YEARS = int(os.getenv("MIN_STOCK_YEARS_LISTED", 5))
MIN_PRICE_CAGR = float(os.getenv("MIN_STOCK_PRICE_CAGR", 0.05))
SECTORS = os.getenv("SECTORS", None) # e.g. "Information Technology,Financial Services"
SECTOR_LIST = [s.strip() for s in SECTORS.split(',')] if SECTORS else None


def years_between(a,b):
    return (b-a).days/365.25

def cagr(s,e,y):
    if s and e and s>0 and e>0 and y>0:
        return (e/s)**(1/y)-1
    
def eval_one(t):
    tk = yf.Ticker(t)
    hist_all = tk.history(period="max")

    if hist_all is None or hist_all.empty:
        return {"ticker": t, "error": "no_history"}

    listed_years = years_between(hist_all.index[0], hist_all.index[-1])
    seasoned = listed_years>=MIN_LISTED_YEARS

    end_dt = datetime.utcnow(); start_dt = end_dt - timedelta(days=LOOKBACK_YEARS*365+14)
    hist = tk.history(start=start_dt, end=end_dt)

    if hist is None or hist.empty:
        return {"ticker": t, "error": "no_recent_prices"}

    price_col = "Adj Close" if "Adj Close" in hist.columns else "Close"
    m = hist[price_col].dropna().resample("M").last()

    if len(m)<2:
        return {"ticker": t, "error": "insufficient_price_data"}

    pcagr = cagr(float(m.iloc[0]), float(m.iloc[-1]), years_between(m.index[0], m.index[-1]))
    shows_growth = pcagr is not None and pcagr>=MIN_PRICE_CAGR

    sector = (tk.info.get("sector") or tk.info.get("industry") or "").strip()
    sector_ok = True if not SECTOR_LIST else any(s.lower() in sector.lower() for s in SECTOR_LIST)

    rules = []
    if seasoned: rules.append("seasoned_>=years")
    if shows_growth: rules.append("price_growth")
    if sector_ok and SECTOR_LIST: rules.append("sector_match")

    return {
        "ticker": t,
        "name": tk.info.get("shortName", ""),
        "sector": sector,
        "listed_years": listed_years,
        "price_cagr": pcagr,
        "met_rules": ",".join(rules),
    }

def main():
    # Use NSE EQ by default
    q = {"exchange": "NSE", "series": {"$in": ["EQ", ""]}}
    cur = col("universe_stocks").find(q, {"symbol":1, "_id":0}).limit(int(os.getenv("MAX_TICKERS", "0")) or 0)
    tickers = [d["symbol"] for d in cur]
    rows = [eval_one(t) for t in tickers]
    df = pd.DataFrame(rows)
    df["rules_matched"] = df["met_rules"].apply(lambda s: len(s.split(",")) if isinstance(s,str) and s else 0)
    df.sort_values(["rules_matched","price_cagr","listed_years"], ascending=[False,False,False], inplace=True)
    os.makedirs("output", exist_ok=True)
    df.to_csv(OUT, index=False)
    print(f"Saved {len(df)} stock rows to {OUT}")


if __name__ == "__main__":
    main()