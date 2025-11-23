"""Read ETFs from Mongo, fetch stats via yfinance, evaluate rules, rank, and write CSV."""
import os
import pandas as pd
import numpy as np
import yfinance as yf
from datetime import datetime, timedelta
from common.mongo_utils import col


OUT = os.getenv("OUT_ETFS", "output/etf_results.csv")
LOOKBACK_YEARS = int(os.getenv("LOOKBACK_YEARS", 5))
MIN_CONSISTENT_YEARS = int(os.getenv("MIN_CONSISTENT_YEARS", 4))
MIN_PRICE_CAGR = float(os.getenv("MIN_PRICE_CAGR", 0.03))
MIN_DIV_CAGR = float(os.getenv("MIN_DIV_CAGR", 0.03))


def annualize_cagr(s, e, years):
    if s and e and s>0 and e>0 and years>0:
        return (e/s)**(1/years)-1




def calendar_year_dividends(divs):
    if divs is None or len(divs)==0:
        return pd.Series(dtype=float)
    s = divs.copy()
    s.index = pd.to_datetime(s.index)
    return s.groupby(s.index.year).sum().sort_index()


def evaluate(ticker):
    tk = yf.Ticker(ticker)
    end_dt = datetime.utcnow()
    start_dt = end_dt - timedelta(days=LOOKBACK_YEARS*365 + 14)
    hist = tk.history(start=start_dt, end=end_dt, auto_adjust=False)

    if hist is None or hist.empty:
        return {"ticker": ticker, "error": "no_history"}
    
    price_col = "Adj Close" if "Adj Close" in hist.columns else "Close"
    m = hist[price_col].dropna().resample("M").last()

    if len(m)<2:
        return {"ticker": ticker, "error": "insufficient_price_data"}
    
    years = (m.index[-1]-m.index[0]).days/365.25
    pcagr = annualize_cagr(float(m.iloc[0]), float(m.iloc[-1]), years)

    annual = calendar_year_dividends(tk.dividends)
    yrs = list(range(datetime.utcnow().year-LOOKBACK_YEARS+1, datetime.utcnow().year+1))
    annual = annual.reindex(yrs, fill_value=0.0)
    consistent_years = int((annual>0).sum())

    dcagr = None
    if len(annual)>=2 and annual.iloc[0]>0 and annual.iloc[-1]>0 and not (annual<=0).all():
        dcagr = annualize_cagr(float(annual.iloc[0]), float(annual.iloc[-1]), len(annual)-1)
    recent_non_decr = None if len(annual)<2 else float(annual.iloc[-1])>=float(annual.iloc[-2])

    meets_consistency = consistent_years>=MIN_CONSISTENT_YEARS
    meets_price = pcagr is not None and pcagr>=MIN_PRICE_CAGR
    meets_div_growth = (dcagr is not None and dcagr>=MIN_DIV_CAGR and (recent_non_decr is True))

    rules = []
    if meets_consistency: rules.append("dividend_consistency")
    if meets_price: rules.append("principal_growth")
    if meets_div_growth: rules.append("dividend_growth")


    return {
        "ticker": ticker,
        "name": tk.info.get("shortName", ""),
        "price_cagr": pcagr,
        "dividend_cagr": dcagr,
        "consistent_years": consistent_years,
        "recent_div_non_decreasing": recent_non_decr,
        "met_rules": ",".join(rules),
    }

def main():
    etfs = list(col("universe_etfs").find({}, {"symbol":1, "_id":0}))
    tickers = [d["symbol"] for d in etfs]
    rows = [evaluate(t) for t in tickers]
    df = pd.DataFrame(rows)
    df["rules_matched"] = df["met_rules"].apply(lambda s: len(s.split(",")) if isinstance(s,str) and s else 0)
    df.sort_values(["rules_matched","price_cagr","dividend_cagr","consistent_years"], ascending=[False,False,False,False], inplace=True)
    os.makedirs("output", exist_ok=True)
    df.to_csv(OUT, index=False)
    print(f"Saved {len(df)} ETF rows to {OUT}")


if __name__ == "__main__":
    main()

















