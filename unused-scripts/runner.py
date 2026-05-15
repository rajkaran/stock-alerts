import os
import json
import math
import time
import logging
from logging.handlers import TimedRotatingFileHandler
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from dotenv import load_dotenv
import pandas as pd
import yfinance as yf

load_dotenv()  # load .env if present

# ===================== CONFIG =====================
TICKERS = [
    "BCE.TO","BNS.TO","CM.TO","CSH-UN.TO","ENB.TO",
    "FIE.TO","POW.TO","SGR-UN.TO","SRU-UN.TO","T.TO","TD.TO","FTS.TO"
]
HISTORY_DAYS = 180
TORONTO_TZ = ZoneInfo("America/Toronto")
DATA_DIR = "data"
LOG_DIR = "logs"

# Email (set via env or hardcode for quick test)
EMAIL_ENABLED = os.getenv("EMAIL_ENABLED", "0") == "1"
SMTP_HOST = os.getenv("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
EMAIL_USER = os.getenv("EMAIL_USER", "")
EMAIL_PASS = os.getenv("EMAIL_PASS", "")
EMAIL_TO   = os.getenv("EMAIL_TO", EMAIL_USER)  # send to yourself by default

# ===================== LOGGING =====================
os.makedirs(LOG_DIR, exist_ok=True)
logger = logging.getLogger("stocks")
logger.setLevel(logging.INFO)
h = TimedRotatingFileHandler(
    filename=os.path.join(LOG_DIR, "run.log"),
    when="midnight", backupCount=15, utc=False
)
fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
h.setFormatter(fmt)
logger.addHandler(h)

def prune_old_logs(days=15):
    # TimedRotatingFileHandler already keeps 15 backups, this is just a safety sweep
    cutoff = datetime.now(TORONTO_TZ) - timedelta(days=days)
    for f in os.listdir(LOG_DIR):
        p = os.path.join(LOG_DIR, f)
        if os.path.isfile(p):
            mtime = datetime.fromtimestamp(os.path.getmtime(p), TORONTO_TZ)
            if mtime < cutoff:
                try: os.remove(p)
                except Exception: pass

# ===================== UTILS =====================
def is_market_open_toronto(now=None):
    now = now or datetime.now(TORONTO_TZ)
    if now.weekday() >= 5:  # weekend
        return False
    return (now.time() >= datetime.strptime("09:30","%H:%M").time()
            and now.time() <= datetime.strptime("16:00","%H:%M").time())

def ensure_dirs():
    os.makedirs(DATA_DIR, exist_ok=True)

def multi_flatten(df):
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = ["{}_{}".format(a,b) for a,b in df.columns]
    return df

def col_for(df, prefix, ticker):
    # works for both flat (Close) and flattened (Close_TICKER)
    cands = [c for c in df.columns if c == prefix or c.startswith(prefix + "_")]
    if len(cands) == 1:
        return cands[0]
    # prefer the one that matches ticker explicitly
    for c in cands:
        if c.endswith("_" + ticker):
            return c
    return cands[0] if cands else None

# ===================== DAILY RUN CHECK =====================
def is_first_run_today():
    ensure_dirs()
    marker = os.path.join(DATA_DIR, ".cache_date")
    today = datetime.now(TORONTO_TZ).date().isoformat()
    if not os.path.exists(marker):
        with open(marker, "w") as f: f.write(today)
        return True
    with open(marker, "r") as f:
        last = f.read().strip()
    if last != today:
        with open(marker, "w") as f: f.write(today)
        return True
    return False

# ===================== CACHE (morning init) =====================
def cache_history_if_needed(tickers=TICKERS, days=HISTORY_DAYS):
    """Run this once each morning (or just let first run do it). Saves per-ticker CSV."""
    ensure_dirs()
    
    refresh = is_first_run_today()
    if not refresh:
        logger.info("Using cached history from previous runs today")
        return
    
    logger.info("First run of the day → refreshing all cached history")

    for t in tickers:
        path = os.path.join(DATA_DIR, f"{t.replace('.','_')}_history.csv")

        try:
            df = yf.download(t, period=f"{days}d", interval="1d", auto_adjust=False, progress=False)
            if df.empty:
                logger.warning(f"Empty history for {t}")
                continue
            df = multi_flatten(df).sort_index()
            df.to_csv(path, index=True)
            logger.info(f"Cached history for {t}: {path}")
        except Exception as e:
            logger.exception(f"History fetch failed for {t}: {e}")

def load_history(ticker):
    path = os.path.join(DATA_DIR, f"{ticker.replace('.','_')}_history.csv")
    if not os.path.exists(path):
        return None
    df = pd.read_csv(path, parse_dates=["Date"])
    df.set_index("Date", inplace=True)
    return df

# ===================== LIVE SNAPSHOT =====================
def fetch_intraday_batch(tickers):
    """
    Fetch 1-minute intraday for 'today' for ALL tickers once (efficient).
    Returns a dict: { ticker: DataFrame (today only, 1m bars) }
    If intraday fails, returns empty dict and we can fallback to fast_info for last price.
    """
    try:
        df = yf.download(" ".join(tickers), period="1d", interval="1m", auto_adjust=False, progress=False, group_by="ticker")
        if isinstance(df.columns, pd.MultiIndex):
            # Multi-ticker format: top-level = ticker
            result = {}
            for t in tickers:
                if t in df.columns.levels[0]:
                    sub = df[t].copy()
                    sub = sub.sort_index()
                    # keep only today's bars (sometimes extended data may appear)
                    today = datetime.now(TORONTO_TZ).date()
                    try:
                        sub_idx_tz = sub.index.tz_convert(TORONTO_TZ)
                    except Exception:
                        sub_idx_tz = sub.index  # if tz-naive, keep as is
                    sub_today = sub[sub_idx_tz.date == today]
                    result[t] = sub_today
            return result
        else:
            # Single-ticker response structure
            today = datetime.now(TORONTO_TZ).date()
            idx_tz = df.index
            try:
                idx_tz = df.index.tz_convert(TORONTO_TZ)
            except Exception:
                pass
            df_today = df[idx_tz.date == today]
            return {tickers[0]: df_today}
    except Exception as e:
        logger.warning(f"Intraday batch fetch failed: {e}")
        return {}

def live_snapshot_latest_price(ticker):
    """Fallback quick last price if intraday batch not available."""
    try:
        return float(yf.Ticker(ticker).fast_info.get("last_price") or 0.0)
    except Exception:
        return 0.0

# ===================== METRICS + SCORING =====================
def compute_window_metrics(close_series, low_series, window_days):
    if len(close_series) < window_days or len(low_series) < window_days:
        return None
    recent_close = close_series[-window_days:]
    recent_low   = low_series[-window_days:]
    return {
        "average": float(pd.Series(recent_close).mean()),
        "minimum": float(pd.Series(recent_low).min())
    }

def invest_bucket(latest, m30, m90):
    a30, n30 = m30["average"], m30["minimum"]
    a90, n90 = m90["average"], m90["minimum"]
    # thresholds
    th_30_50 = a30 - 0.5*(a30 - n30)
    th_90_50 = a90 - 0.5*(a90 - n90)
    th_30_80 = a30 - 0.8*(a30 - n30)
    th_90_80 = a90 - 0.8*(a90 - n90)

    # Order = priority (highest invest first)
    # lastes price < minimum of 90 days : invest $800
    if latest < n90:                  return 800, "Below 90d min"
    # latest price < minimum of 30 days : invest $600
    elif latest < n30:                return 600, "Below 30d min"
    # latest price < 80% gap (90d) : invest $700
    elif latest < th_90_80:           return 700, "Below 80% gap (90d)"
    # latest price < 80% gap (30d) : invest $500
    elif latest < th_30_80:           return 500, "Below 80% gap (30d)"
    # latest price < 50% gap (90d) : invest $500
    elif latest < th_90_50:           return 500, "Below 50% gap (90d)"
    # latest price < 50% gap (30d) : invest $400
    elif latest < th_30_50:           return 400, "Below 50% gap (30d)"
    # latest price < average 90d : invest $200
    elif latest < a90:                return 200, "Below 90d avg"
    # latest price < average 30d : invest $100
    elif latest < a30:                return 100, "Below 30d avg"
    else:                             return 0,   "No signal"

def amount_to_color(amount):
    # map {0,100,200,400,500,600,700,800} to 0..1
    steps = [0,100,200,400,500,600,700,800]
    norm = steps.index(amount)/ (len(steps)-1) if amount in steps else 0.0
    r = int((1.0 - norm) * 255)
    g = int(norm * 255)
    return f"rgb({r},{g},0)"

# ===================== REPORT =====================
def build_html_table(rows):
    """
    rows: list of dicts with keys:
      ticker, latest, avg30, min30, avg90, min90, half30, eighty30, half90, eighty90, label, amount
    """
    head = """
    <table border="1" cellspacing="0" cellpadding="6" style="border-collapse:collapse;font-family:Arial;font-size:12px;">
      <thead style="background:#f0f0f0">
        <tr>
          <th>Ticker</th><th>Latest</th><th>Min 90d</th><th>80%Gap 90d</th><th>HalfGap 90d</th>
          <th>Min 30d</th><th>80%Gap 30d</th><th>Avg 90d</th><th>HalfGap 30d</th><th>Avg 30d</th>          
          <th>Decision</th><th>Amount</th>
        </tr>
      </thead>
      <tbody>
    """
    body = ""
    for r in rows:
        body += f"""
        <tr>
          <td>{r['ticker']}</td>
          <td>{r['latest']:.2f}</td>
          <td>{r['min90']:.2f}</td>
          <td>{r['eighty90']:.2f}</td>
          <td>{r['half90']:.2f}</td>
          <td>{r['min30']:.2f}</td>
          <td>{r['eighty30']:.2f}</td>
          <td>{r['avg90']:.2f}</td>
          <td>{r['half30']:.2f}</td>
          <td>{r['avg30']:.2f}</td>
          <td style="background:{r['color']};text-align:center;"><b>{r['label']}</b></td>
          <td style="background:{r['color']};text-align:center;"><b>${r['amount']}</b></td>
        </tr>
        """
    tail = "</tbody></table>"
    return head + body + tail

def send_email(html):
    if not EMAIL_ENABLED:
        logger.info("EMAIL_ENABLED=0; skipping email send.")
        return
    import smtplib
    from email.mime.text import MIMEText
    msg = MIMEText(html, "html")
    msg["Subject"] = f"Stock Signals — {datetime.now(TORONTO_TZ).strftime('%Y-%m-%d %H:%M %Z')}"
    msg["From"] = EMAIL_USER
    msg["To"] = EMAIL_TO
    with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as s:
        s.starttls()
        s.login(EMAIL_USER, EMAIL_PASS)
        s.send_message(msg)

# ===================== MAIN RUN =====================
def run_once():
    """
    Run one evaluation cycle:
    - Refresh history once per day
    - Load cached 180-day history per ticker
    - Merge today's intraday stats
    - Compute metrics and invest signal
    - Return results DataFrame
    """

    start = datetime.now(TORONTO_TZ)
    ok = True
    try:
        cache_history_if_needed(TICKERS, HISTORY_DAYS)

        # Batch intraday for today (efficient)
        intra = fetch_intraday_batch(TICKERS)

        print(intra)

        results = []
        for t in TICKERS:
            hist = load_history(t)
            if hist is None or hist.empty:
                logger.warning(f"No history for {t}")
                continue

            # Identify Close/Low columns
            hist = multi_flatten(hist).sort_index()
            close_col = col_for(hist, "Close", t)
            low_col   = col_for(hist, "Low", t)
            if not close_col or not low_col:
                logger.warning(f"Missing columns for {t}")
                continue

            # Extract arrays
            close = hist[close_col].astype(float).tolist()
            low   = hist[low_col].astype(float).tolist()

            # include today's data:
            # if we got intraday for today, use its last price and min + avg (for completeness we’ll use last price as latest)
            latest = None
            close_today_mean = None
            low_today_min = None
            if t in intra and not intra[t].empty:
                sub = intra[t]
                c_close = sub["Close"].dropna()
                c_low   = sub["Low"].dropna()

                if not c_close.empty:
                    latest = float(c_close.iloc[-1])
                    close_today_mean = float(c_close.mean())
                if not c_low.empty:
                    low_today_min = float(c_low.min())

            # --- fallback to fast_info snapshot ---
            if latest is None or math.isnan(latest):
                latest = live_snapshot_latest_price(t)

            # --- final fallback: last historical close ---
            if latest is None or math.isnan(latest):
                latest = float(close[-1]) if close else 0.0
                
            # if last hist row is already today, overwrite, else append
            last_idx_date = hist.index[-1].date()
            today_date = datetime.now(TORONTO_TZ).date()

            if close_today_mean is not None and low_today_min is not None:
                if last_idx_date == today_date:
                    close[-1] = close_today_mean
                    low[-1]   = low_today_min
                else:
                    close.append(close_today_mean)
                    low.append(low_today_min)            

            # --- compute metrics ---
            m30 = compute_window_metrics(close, low, 30)
            m90 = compute_window_metrics(close, low, 90)
            if not m30 or not m90:
                logger.warning(f"Insufficient data for {t}")
                continue

            # --- thresholds for table ---
            half_30   = m30["average"] - 0.5*(m30["average"] - m30["minimum"])
            eighty_30 = m30["average"] - 0.8*(m30["average"] - m30["minimum"])
            half_90   = m90["average"] - 0.5*(m90["average"] - m90["minimum"])
            eighty_90 = m90["average"] - 0.8*(m90["average"] - m90["minimum"])

            # --- priority invest signal ---
            amount, label = invest_bucket(latest, m30, m90)
            color = amount_to_color(amount)

            results.append({
                "ticker": t,
                "latest": latest,
                "avg30": m30["average"], "min30": m30["minimum"],
                "avg90": m90["average"], "min90": m90["minimum"],
                "half30": half_30, "eighty30": eighty_30,
                "half90": half_90, "eighty90": eighty_90,
                "amount": amount, "label": label,
                "color": color
            })

        # Build and (optionally) email the table
        html = build_html_table(results)
        send_email(html)

        # Log a short success line
        logger.info(f"Run OK — tickers={len(results)} emailed={'yes' if EMAIL_ENABLED else 'no'}")

    except Exception as e:
        ok = False
        logger.exception(f"Run FAILED: {e}")

    finally:
        # prune old logs
        prune_old_logs(15)
        elapsed = (datetime.now(TORONTO_TZ) - start).total_seconds()
        logger.info(f"Elapsed: {elapsed:.2f}s; Status: {'OK' if ok else 'FAIL'}")

if __name__ == "__main__":
    run_once()
