import yfinance as yf
from datetime import datetime, timedelta
import pandas as pd

# ==== CONFIG ====
TICKERS = ["BCE.TO","BNS.TO","CM.TO","CSH-UN.TO","ENB.TO","FIE.TO","POW.TO","SGR-UN.TO","SRU-UN.TO","T.TO","TD.TO","FTS.TO"] # Canadian tickers (TSX)

def fetch_history(ticker, days=180):
    """Fetch last N days of daily OHLCV data. So the results are from previous day to N days ago. Aftermarket closes then it will give todays data. As its frozen now."""
    df = yf.download(ticker, period=f"{days}d", interval="1d", auto_adjust=False)

    if df.empty:
        print(f"Error fetching {ticker}")
        # TODO: send out an email when no data is fetched from yahoo finance
        return []

    # Flatten the multi-index columns
    # ['Adj Close_BNS.TO', 'Close_BNS.TO', 'High_BNS.TO', 'Low_BNS.TO', 'Open_BNS.TO', 'Volume_BNS.TO']
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = ["{}_{}".format(price, tkr) for price, tkr in df.columns]

    # Ensure the dataframe is sorted
    df = df.sort_index()

    # Find the correct Close and Low columns
    close_col = [c for c in df.columns if c.startswith("Close_")][0]
    low_col   = [c for c in df.columns if c.startswith("Low_")][0]

    candles = list(zip(df.index, df[close_col].astype(float), df[low_col].astype(float)))
    return candles # its an array of tupples containing: date, close, low

def compute_metrics(candles, window_days):
    """Compute average close and minimum low over a window."""
    if len(candles) < window_days:
        return None

    recent = candles[-window_days:]
    closes = [c[1] for c in recent]
    lows = [c[2] for c in recent]
    return {
        "average": sum(closes) / len(closes),
        "minimum": min(lows)
    }

def check_alerts(ticker, candles):
    """Check alert conditions for 30 and 90 days."""
    if not candles:
        return

    # for i in range(len(candles)):
    #     print(f"\n --- last day's start and end  {ticker} ---   {candles[i]}")

    # print(f"\n --- checking alerts for {ticker} --- {candles[-1]}")

    latest_price = candles[-1][1]

    metrics_30 = compute_metrics(candles, 30)
    metrics_90 = compute_metrics(candles, 90)

    print(f"\n --- in last 30 days  {ticker} ---   {metrics_30} --- in last 90 days ----   {metrics_90}  ---- latest row: {candles[-1]}")

    alerts = []

    # --- 30 days ---
    if metrics_30:
        avg30, min30 = metrics_30["average"], metrics_30["minimum"]
        if latest_price < avg30:
            alerts.append("Below 30-day average")
        if latest_price < (avg30 - (avg30 - min30) * 0.5):
            alerts.append("Below 50% between avg & min (30d)")
        if latest_price < (avg30 - (avg30 - min30) * 0.8):
            alerts.append("Below 80% between avg & min (30d)")
        if latest_price < min30:
            alerts.append("Below 30-day minimum")

    # --- 90 days ---
    if metrics_90:
        avg90, min90 = metrics_90["average"], metrics_90["minimum"]
        if latest_price < avg90:
            alerts.append("Below 90-day average")
        if latest_price < (avg90 - (avg90 - min90) * 0.5):
            alerts.append("Below 50% between avg & min (90d)")
        if latest_price < (avg90 - (avg90 - min90) * 0.8):
            alerts.append("Below 80% between avg & min (90d)")
        if latest_price < min90:
            alerts.append("Below 90-day minimum")

    print(f"\n[{ticker}] Latest price: {latest_price}")
    for a in alerts:
        print(f"  ⚠️ {a}")

# ==== MAIN ====
if __name__ == "__main__":
    for ticker in TICKERS:
        candles = fetch_history(ticker, days=180)  # 6 months
        check_alerts(ticker, candles)
