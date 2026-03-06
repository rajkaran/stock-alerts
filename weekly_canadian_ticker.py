from __future__ import annotations

import os
import logging
from datetime import datetime, timedelta, timezone, time
from dotenv import load_dotenv
from zoneinfo import ZoneInfo
import pandas as pd
import yfinance as yf
from pymongo import MongoClient, UpdateOne
from typing import Dict, List, Any
from email.message import EmailMessage
import smtplib
from helper import parse_tickers  # same helper you already have

load_dotenv()

# ------------------- CONFIG -------------------

TZ = ZoneInfo(os.getenv("TZ", "America/Toronto"))

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.getenv("MONGO_DB", "stockdb")

TICKERS = parse_tickers(
    os.getenv("TICKERS"),
    default=["BCE.TO", "BNS.TO"],
)

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("analyze_template")

print(f"Using tickers: {TICKERS}")

# ------------------- MONGO --------------------

client = MongoClient(MONGO_URI)
db = client[MONGO_DB]

price_5m_col = db["PriceFor5MinuteInterval"]
daily_log_col = db["DailyLog"]
exec_for_week_col = db["ExecutionComparedToWeekCanada"]
notify_email_col = db["NotifyEmail"]
email_log_col = db["EmailLog"]

# --------------------- Email config (set these in your .env) --------------------

SMTP_HOST = os.getenv("SMTP_HOST", "")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))  # default TLS port
SMTP_USER = os.getenv("EMAIL_USER", "")
SMTP_PASSWORD = os.getenv("EMAIL_PASS", "")
EMAIL_FROM = os.getenv("EMAIL_FROM", "no-reply@iamraj.com")
EMAIL_SUBJECT = os.getenv("EMAIL_SUBJECT", "Favorable stocks to invest on ")

# ------------------- WEEK FIELD NAMES --------------------

# These are the same week spans used in fetch_minimum_close_byweek_for_ticker
WEEK_SPANS = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14]

# All possible week facet names saved into ExecutionComparedToWeekCanada
WEEK_FIELDS = ["sinceThisWeek"] + [f"sincePast{w}Weeks" for w in WEEK_SPANS]


# ------------------- YFINANCE HELPERS --------------------

def flatten_yf_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Flatten MultiIndex columns from yfinance into simple strings like 'Open', 'Close', etc."""
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [str(col[0]) for col in df.columns]
    return df


def download_yesterday_5m(ticker: str) -> pd.DataFrame:
    """
    Download 5-minute data for *yesterday only* for a ticker.

    Strategy:
      - Get last 2 days of 5m data from yfinance.
      - Convert index to UTC.
      - Filter rows whose local date (in TZ) == yesterday.
    """
    log.info("%s: downloading 5m for last 2 days", ticker)
    df = yf.download(
        ticker,
        period="2d",
        interval="5m",
        auto_adjust=False,
        progress=False,
        group_by="column",
    )

    if df is None or df.empty:
        log.warning("%s: no 5m data returned for last 2 days", ticker)
        return pd.DataFrame()

    df = flatten_yf_columns(df)

    # Ensure tz-aware UTC index
    idx = pd.to_datetime(df.index, utc=True, errors="coerce")
    df = df.loc[~idx.isna()].copy()
    df.index = idx[~idx.isna()]

    # Filter only yesterday (in local TZ)
    now_local = datetime.now(TZ)
    yesterday_date = (now_local - timedelta(days=1)).date()

    local_dates = df.index.tz_convert(TZ).date
    mask = local_dates == yesterday_date
    df_yesterday = df.loc[mask]

    if df_yesterday.empty:
        log.warning("%s: no 5m bars for yesterday (%s)", ticker, yesterday_date)
    else:
        log.info(
            "%s: got %d 5m bars for yesterday (%s)",
            ticker, len(df_yesterday), yesterday_date,
        )

    return df_yesterday


def fetch_current_price(ticker: str) -> float | None:
    """
    Fetch current (or most recent) price for a ticker.

    Uses 1d of 1m data and returns the last non-NaN Close.
    """
    df = yf.download(
        ticker,
        period="1d",
        interval="1m",
        auto_adjust=False,
        progress=False,
        group_by="column",
    )

    if df is None or df.empty:
        log.warning("%s: no intraday data for current price", ticker)
        return None

    df = flatten_yf_columns(df)

    if "Close" not in df.columns:
        log.warning("%s: 'Close' column missing in current price data", ticker)
        return None

    closes = df["Close"].dropna()
    if closes.empty:
        log.warning("%s: no non-NaN Close values for current price", ticker)
        return None

    price = float(closes.iloc[-1])
    log.info("%s: current price = %f", ticker, price)
    return price


# ------------------- UPSERT YESTERDAY 5M --------------------

def upsert_5m_raw(ticker: str, df: pd.DataFrame) -> int:
    """
    Save 5-minute bars to MongoDB for a ticker.

    Fields per document:
      - ticker
      - ts (UTC datetime)
      - createDatetime (UTC datetime when we write)
      - plus all yfinance fields: Open, High, Low, Close, Adj Close, Volume, etc.
    """
    if df is None or df.empty:
        log.info("%s: nothing to upsert (empty DataFrame)", ticker)
        return 0

    created_at = datetime.now(timezone.utc)
    ops: list[UpdateOne] = []

    for ts, row in df.iterrows():
        base = {str(k): v for k, v in row.to_dict().items()}
        base["ticker"] = ticker
        base["ts"] = ts.to_pydatetime()  # aware UTC datetime
        base["createDatetime"] = created_at

        ops.append(
            UpdateOne(
                {"ticker": ticker, "ts": base["ts"]},
                {"$set": base},
                upsert=True,
            )
        )

    if not ops:
        log.info("%s: no ops generated", ticker)
        return 0

    result = price_5m_col.bulk_write(ops, ordered=False)
    upserted = (result.upserted_count or 0) + (result.modified_count or 0)
    log.info("%s: upserted/modified %d 5m bars", ticker, upserted)
    return upserted


def fetch_yesterday_for_all_tickers():
    """Fetch yesterday's 5m data for all tickers and store in PriceFor5MinuteInterval."""
    for t in TICKERS:
        try:
            df = download_yesterday_5m(t)
            upsert_5m_raw(t, df)
        except Exception as e:
            log.exception("%s: failed to fetch/upsert yesterday's data: %s", t, e)


# ------------------- DAILY LOG --------------------

DAILY_LOG_ID = "singleton"  # so we always have exactly one document


def get_last_update_local_date():
    doc = daily_log_col.find_one({"_id": DAILY_LOG_ID})
    if not doc or "lastUpdateDatetime" not in doc:
        return None

    dt = doc["lastUpdateDatetime"]
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(TZ).date()


def update_daily_log_now():
    now_utc = datetime.now(timezone.utc)
    daily_log_col.update_one(
        {"_id": DAILY_LOG_ID},
        {"$set": {"lastUpdateDatetime": now_utc}},
        upsert=True,
    )
    log.info("DailyLog updated with lastUpdateDatetime=%s", now_utc.isoformat())


def ensure_yesterday_data_if_needed():
    """
    If DailyLog.lastUpdateDatetime is not today (local) or missing:
      - fetch yesterday's 5m data for all tickers
      - update DailyLog to now
    """
    today_local = datetime.now(TZ).date()
    last_update_date = get_last_update_local_date()

    if last_update_date == today_local:
        log.info("DailyLog is up-to-date for today (%s), skipping fetch.", today_local)
        return

    log.info(
        "DailyLog lastUpdate=%s, today=%s -> fetching yesterday's 5m data.",
        last_update_date,
        today_local,
    )
    fetch_yesterday_for_all_tickers()
    update_daily_log_now()


# ------------------- Query Tickers and Save Pricing State --------------------

def local_9am_to_utc(d):
    """
    Given a date in local (Toronto) terms, return that date's 09:00
    in UTC, suitable for querying Mongo where 'ts' is stored as UTC.
    """
    local_dt = datetime.combine(d, time(9, 0), tzinfo=TZ)
    return local_dt.astimezone(timezone.utc)

def fetch_minimum_close_byweek_for_ticker() -> dict:
    """
    Compute minimum Close per ticker over multiple week windows.

    Returns a dict like:
    {
        "sinceThisWeek": { "BCE.TO": 44.10, "BNS.TO": 56.20, ... },
        "sincePast1Weeks": { "BCE.TO": 43.90, ... },
        "sincePast2Weeks": { ... },
        ...
    }
    """
    now_utc = datetime.now(timezone.utc)
    now_local = now_utc.astimezone(TZ)
    today_local = now_local.date()

    # This week Monday (local)
    monday_local = today_local - timedelta(days=today_local.weekday())

    # "since past N weeks (till today)" = from (monday_of_this_week - N weeks)
    week_ranges_utc = {}

    for w in WEEK_SPANS:
        start_date_local = monday_local - timedelta(weeks=w)
        week_ranges_utc[w] = start_date_local

    oldest_week = WEEK_SPANS[-1]
    
    # Base pipeline with global lower bound (oldest week @ 9am local)
    pipeline_week_intervals = [
        {
            "$match": {
                "ts": {
                    "$gte": local_9am_to_utc(week_ranges_utc[oldest_week]), 
                }
            }
        },
        {
            "$facet": {
                "sinceThisWeek": [
                    {"$match": {"ts": {"$gte": local_9am_to_utc(monday_local)}}},
                    {"$group": {"_id": "$ticker", "minimumClose": {"$min": "$Close"}}}
                ],
            }
        }
    ]

    # Add facets for each N-week span
    for w in WEEK_SPANS:
        pipeline_week_intervals[1]["$facet"][f"sincePast{w}Weeks"] = [
            {"$match": {"ts": {"$gte": local_9am_to_utc(week_ranges_utc[w])}}},
            {"$group": {"_id": "$ticker", "minimumClose": {"$min": "$Close"}}}
        ]

    log.info("pipeline_week_intervals = %s", pipeline_week_intervals)
    
    agg_result = list(price_5m_col.aggregate(pipeline_week_intervals))

    if not agg_result:
        return {}

    facet_doc = agg_result[0]  # single document from $facet

    # Convert arrays per facet → { ticker: minimumClose }
    output: dict[str, dict[str, float]] = {}

    for facet_name, rows in facet_doc.items():
        ticker_map: dict[str, float] = {}
        for row in rows:
            ticker = row.get("_id")
            min_close = row.get("minimumClose")
            if ticker is not None and min_close is not None:
                ticker_map[ticker] = float(min_close)
        output[facet_name] = ticker_map

    return output

def save_week_execution(week_to_tickers: dict[str, list[dict]]) -> str:
    """
    Save the weekly analysis result into exec_for_week_col.

    Document shape:
    {
        _id: ObjectId(...),
        createDatetime: <UTC datetime>,
        weeks: {
            "sinceThisWeek": [ {ticker, currentPrice, minimumPrice}, ... ],
            "sincePast1Weeks": [ ... ],
            ...
        }
    }
    """
    now_utc = datetime.now(timezone.utc)
    
    # Flat document: createDatetime + each week key as its own field
    doc: dict = {"createDatetime": now_utc}
    doc.update(week_to_tickers)

    result = exec_for_week_col.insert_one(doc)
    log.info("Inserted ExecutionComparedToWeekCanada document %s", result.inserted_id)
    return str(result.inserted_id)

def load_previously_reported_pairs() -> set[tuple[str, str]]:
    """
    Scan ExecutionComparedToWeekCanada and collect all
    (ticker, weekFlag) pairs that have been reported *today* (local day).

    This way:
      - Multiple runs in the same day won't re-report the same (ticker, weekFlag).
      - A new day starts fresh.
    """
    pairs: set[tuple[str, str]] = set()

    start_utc, end_utc = get_today_utc_range()

    projection = {field: 1 for field in WEEK_FIELDS}
    projection["createDatetime"] = 1

    cursor = exec_for_week_col.find(
        {
            "createDatetime": {
                "$gte": start_utc,
                "$lt": end_utc,
            },
            "isNotified": True, # only consider notified entries
        },
        projection,
    )

    for doc in cursor:
        for wk in WEEK_FIELDS:
            entries = doc.get(wk, [])
            if not isinstance(entries, list):
                continue

            for e in entries:
                ticker = e.get("ticker")
                if isinstance(ticker, str):
                    pairs.add((ticker, wk))

    log.info(
        "Loaded %d previously reported (ticker, weekFlag) pairs for today",
        len(pairs),
    )
    return pairs


# --------------------- EMAILS ----------------------

def aggregate_week_matches(week_to_tickers: dict[str, list[dict]]) -> List[Dict[str, Any]]:
    """
    From calculated week_to_tickers.   

    For each (weekFlag, ticker) pair, keep the minimum currentPrice across
    all docs and its corresponding compareWith (minimumPrice).

    Returns: list of rows:
      { "ticker": str, "weekFlag": str, "currentPrice": float, "compareWith": float }
    """
    
    data: List[Dict[str, Any]] = []
    for wk in week_to_tickers:
        if(week_to_tickers[wk].__len__() > 0):
            for info in week_to_tickers[wk]:
                data.append(
                    {
                        "ticker": info["ticker"],
                        "weekFlag": wk,
                        "currentPrice": info["currentPrice"],
                        "compareWith": info["minimumPrice"],
                    }
                )               

    return data

def get_notify_emails() -> List[str]:
    """
    Fetch recipient email addresses from NotifyEmail collection.

    Assumes either:
      - Each document has an 'email' field (string), or
      - One document has an 'emails' array.
    We'll collect both if present.
    """
    recipients: List[str] = []

    for doc in notify_email_col.find({}):
        email = doc.get("email")
        if isinstance(email, str):
            recipients.append(email)

        emails_list = doc.get("emails")
        if isinstance(emails_list, list):
            for e in emails_list:
                if isinstance(e, str):
                    recipients.append(e)

    # Deduplicate
    recipients = sorted(set(recipients))
    log.info("NotifyEmail -> %d unique recipients", len(recipients))
    return recipients

def format_table_text(rows: List[Dict[str, Any]]) -> str:
    """Plain-text version as fallback if HTML isn't supported."""
    if not rows:
        return "No weekly signals to report."

    lines = []
    lines.append("Ticker | Week flag | Current price | Compared with")
    lines.append("-----------------------------------------------------------")
    for r in rows:
        lines.append(
            f"{r['ticker']} | {r['weekFlag']} | "
            f"{r['currentPrice']:.4f} | {r['compareWith']:.4f}"
        )

    return "\n".join(lines)

def format_table_html(rows: List[Dict[str, Any]]) -> str:
    """Format the aggregated rows into an HTML table for the email body."""
    if not rows:
        # Not expected to be called in that case, but safe anyway
        return """
        <html>
          <body style="font-family: Arial, sans-serif; font-size: 14px;">
            <p>No weekly signals to report.</p>
          </body>
        </html>
        """

    # Build table rows
    row_html_parts = []
    for r in rows:
        row_html_parts.append(f"""
          <tr>
            <td style="border:1px solid #ccc; padding:4px 8px;">{r['ticker']}</td>
            <td style="border:1px solid #ccc; padding:4px 8px;">{r['weekFlag']}</td>
            <td style="border:1px solid #ccc; padding:4px 8px; text-align:right;">
              {r['currentPrice']:.4f}
            </td>
            <td style="border:1px solid #ccc; padding:4px 8px; text-align:right;">
              {r['compareWith']:.4f}
            </td>
          </tr>
        """)

    rows_html = "\n".join(row_html_parts)

    # Full HTML document
    html = f"""
    <html>
      <body style="font-family: Arial, sans-serif; font-size: 14px;">
        <p>Here's the list of Stocks favorable to invest in at the moment:</p>
        <table style="border-collapse: collapse; border:1px solid #ccc;">
          <thead>
            <tr style="background-color:#f2f2f2;">
              <th style="border:1px solid #ccc; padding:4px 8px; text-align:left;">Ticker</th>
              <th style="border:1px solid #ccc; padding:4px 8px; text-align:left;">Week flag</th>
              <th style="border:1px solid #ccc; padding:4px 8px; text-align:right;">Current price</th>
              <th style="border:1px solid #ccc; padding:4px 8px; text-align:right;">Compared with</th>
            </tr>
          </thead>
          <tbody>
            {rows_html}
          </tbody>
        </table>
      </body>
    </html>
    """
    return html

def send_email(recipients: List[str], subject: str, body_text: str, body_html: str | None = None) -> bool:
    """
    Send an email with the given subject/body to recipients using SMTP settings.
    Returns True if send was attempted and did not raise, False otherwise.
    """
    if not recipients:
        log.warning("No recipients specified, skipping email send.")
        return False

    if not SMTP_HOST:
        log.error("SMTP_HOST is not configured, cannot send email.")
        return False

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = EMAIL_FROM
    msg["To"] = ", ".join(recipients)

    # Plain-text part
    msg.set_content(body_text)

    # HTML alternative (if provided)
    if body_html:
        msg.add_alternative(body_html, subtype="html")

    try:
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
            server.starttls()
            if SMTP_USER and SMTP_PASSWORD:
                server.login(SMTP_USER, SMTP_PASSWORD)
            server.send_message(msg)
        log.info("Email sent to: %s", msg["To"])
        return True
    except Exception as e:
        log.exception("Failed to send email: %s", e)
        return False
    
def log_email_send(recipients: List[str], subject: str, row_count: int, insertedId: str = ""):
    """Insert a log entry into EmailLog."""
    now_utc = datetime.now(timezone.utc)
    doc = {
        "createDatetime": now_utc,
        "subject": subject,
        "recipients": recipients,
        "rowCount": row_count,
        "executionId": insertedId,
        "type": "weeklySignals",
    }
    email_log_col.insert_one(doc)
    log.info("EmailLog entry created (%d rows)", row_count)    

# ------------------- ANALYSIS / STATS --------------------

# Define ordering: check *oldest* windows first,
# so a ticker goes into the longest lookback where it qualifies.
def week_sort_key(name: str) -> int:
    if name == "sinceThisWeek":
        return 0  # treat as "0 weeks back"
    if name.startswith("sincePast") and name.endswith("Weeks"):
        # "sincePast14Weeks" -> 14
        num_str = name[len("sincePast") : -len("Weeks")]
        try:
            return int(num_str)
        except ValueError:
            return 0
    return 0

def get_today_utc_range():
    """
    Return (start_utc, end_utc) for *today* in local TZ (America/Toronto).
    Used to limit dedupe to the current day.
    """
    now_local = datetime.now(TZ)
    today = now_local.date()

    start_local = datetime.combine(today, time(0, 0, 0), tzinfo=TZ)
    end_local = start_local + timedelta(days=1)

    start_utc = start_local.astimezone(timezone.utc)
    end_utc = end_local.astimezone(timezone.utc)

    return start_utc, end_utc

def filter_already_reported_week_signals(
    week_to_tickers: dict[str, list[dict]]
) -> dict[str, list[dict]]:
    """
    Given the freshly computed week_to_tickers, drop any entries where
    (ticker, weekFlag) has already been reported in the past.

    - If a ticker shows up again in the *same* weekFlag, we suppress it.
    - If it moves into an *older* weekFlag (e.g. sincePast4Weeks -> sincePast6Weeks),
      that (ticker, weekFlag) pair is new, so we keep it.
    """
    previously_reported = load_previously_reported_pairs()

    filtered: dict[str, list[dict]] = {}
    for wk, entries in week_to_tickers.items():
        new_entries: list[dict] = []
        for info in entries:
            ticker = info.get("ticker")
            if not isinstance(ticker, str):
                continue

            key = (ticker, wk)
            if key in previously_reported:
                log.info(
                    "Skipping %s in %s (already reported previously)",
                    ticker,
                    wk,
                )
                continue

            new_entries.append(info)

        filtered[wk] = new_entries

    return filtered

def run_base_analysis():
    """
    Base flow:
      1) Ensure yesterday's data is present.
      2) Compute minimum Close per ticker for multiple week windows.
      3) Fetch current price for each ticker.
      4) For each ticker, find the *oldest* week window where
         currentPrice < that week's minimum close, and assign
         the ticker only to that week.
      5) Return a structured dict with everything.
    """
    # 1) Make sure historical data is up to date
    ensure_yesterday_data_if_needed()
    
    # 2) Get min close by interval (facet result)
    min_by_interval = fetch_minimum_close_byweek_for_ticker()
    
    # We want *descending* (oldest weeks first)
    ordered_week_keys = sorted(min_by_interval.keys(), key=week_sort_key, reverse=True)

    log.info("Ordered week keys: %s", ordered_week_keys)

    # Init all weeks with empty lists so it's okay if some remain empty
    week_to_tickers: dict[str, list[dict]] = {
        wk: [] for wk in ordered_week_keys
    }

    # 3) Get current price per ticker
    for ticker in TICKERS:
        current = fetch_current_price(ticker)

        if current is None:
            log.warning("%s: current price is None, skipping.", ticker)
            continue

        for wk in ordered_week_keys:
            minimum_price = min_by_interval.get(wk).get(ticker)
            
            if minimum_price is None:
                # no data for this ticker in this week span
                continue

            if(current < minimum_price):
                week_to_tickers[wk].append({
                    "ticker": ticker,
                    "currentPrice": current,
                    "minimumPrice": minimum_price,
                })
                break  # only assign to the oldest qualifying week

    log.info("=== Raw Analysis Results === %s", week_to_tickers)

    # 5) filter out already reported tickers
    filtered_week_to_tickers = filter_already_reported_week_signals(week_to_tickers)

    log.info("=== Filtered (new) Analysis Results === %s", filtered_week_to_tickers)    

    # 5) Send out email if we have any matches
    rows = aggregate_week_matches(filtered_week_to_tickers)

    # Only send email if we have at least one row
    if not rows:
        log.info("No weekly signals to report; email will not be sent.")
        save_week_execution(week_to_tickers)
        return

    # Format table for email
    body_text = format_table_text(rows)
    body_html = format_table_html(rows)

    # Build subject with execution date (local Toronto date)
    now_local = datetime.now(TZ)
    date_str = now_local.strftime("%Y-%m-%d")  # e.g. 2025-12-10
    subject = f"{EMAIL_SUBJECT}{date_str}"

    # Fetch recipients
    recipients = get_notify_emails()

    # Send email
    sent = send_email(recipients, subject, body_text, body_html)

    # Log the email attempt if sent
    if sent:
        # 4) Save to MongoDB
        week_to_tickers['isNotified'] = True        
        insertedId = save_week_execution(week_to_tickers)
        log_email_send(recipients, subject, len(rows), insertedId)
    
    return rows


# ------------------- MAIN --------------------

def main():
    run_base_analysis()


if __name__ == "__main__":
    main()
