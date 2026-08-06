"""
sheet_sync_common.py
---------------------
Shared helpers for the MongoDB -> Google Sheet sync scripts
(sync_trades.py, sync_dividends.py). Keeps sheet/column/cursor logic
in one place so each collection-specific script only has to define its
own field mapping.
"""

from __future__ import annotations

import logging
import os
import random
import time
from datetime import datetime, timezone, timedelta

import gspread
from dotenv import load_dotenv
from google.oauth2.service_account import Credentials
from gspread.exceptions import APIError

# Load .env here so GOOGLE_SHEET_ID etc. below are populated regardless of
# whether the calling script's own load_dotenv() runs before or after its
# `from sheet_sync_common import ...` line (import statements always run
# first, so relying on the caller alone is fragile).
load_dotenv()

# ─────────────────────────── CONFIG ───────────────────────────

GOOGLE_SERVICE_ACCOUNT_JSON = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "service_account.json")
GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID", "")

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

# Retry behaviour for transient Sheets API failures (429 / 5xx / network blips)
RETRY_MAX_ATTEMPTS = int(os.getenv("SHEETS_RETRY_MAX_ATTEMPTS", "5"))
RETRY_BASE_DELAY = float(os.getenv("SHEETS_RETRY_BASE_DELAY", "1.0"))  # seconds
RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}


def get_logger(name: str) -> logging.Logger:
    logging.basicConfig(
        level=getattr(logging, os.getenv("LOG_LEVEL", "INFO"), logging.INFO),
        format="%(asctime)s %(levelname)s %(message)s",
    )
    return logging.getLogger(name)


# ─────────────────────────── RETRY WRAPPER ─────────────────────

def _is_retryable(exc: Exception) -> bool:
    if isinstance(exc, APIError):
        try:
            status = exc.response.status_code
        except AttributeError:
            status = None
        return status in RETRYABLE_STATUS_CODES
    # Transient network-level errors (connection reset, timeout, etc.)
    return isinstance(exc, (ConnectionError, TimeoutError))


def with_retry(func, *args, log: logging.Logger, desc: str = "", **kwargs):
    """
    Call func(*args, **kwargs), retrying with exponential backoff + jitter
    on 429 / 5xx / transient network errors. Re-raises immediately for
    anything else (bad sheet id, auth failure, etc. shouldn't be retried).
    """
    attempt = 0
    while True:
        attempt += 1
        try:
            return func(*args, **kwargs)
        except Exception as exc:
            if attempt >= RETRY_MAX_ATTEMPTS or not _is_retryable(exc):
                raise
            delay = RETRY_BASE_DELAY * (2 ** (attempt - 1)) + random.uniform(0, 0.5)
            log.warning(
                "%s failed (attempt %d/%d): %s — retrying in %.1fs",
                desc or getattr(func, "__name__", "call"), attempt, RETRY_MAX_ATTEMPTS, exc, delay,
            )
            time.sleep(delay)


# ─────────────────────────── SERIALIZATION ────────────────────

def serialize(value) -> str:
    """Convert any MongoDB value to a plain string safe for Google Sheets."""
    if value is None:
        return ""
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.isoformat()
    # Handles ObjectId, Decimal128, bool, int, float, str, etc.
    return str(value)


def monday_of_week(dt: datetime) -> str:
    """Return the Monday date of the week containing dt, as YYYY-MM-DD string."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    # weekday(): Monday=0, Sunday=6
    monday = dt - timedelta(days=dt.weekday())
    return monday.strftime("%Y-%m-%d")


# ─────────────────────────── COLUMN MANAGEMENT ────────────────

def merge_columns(existing: list[str], incoming: set[str], preferred_order: list[str]) -> list[str]:
    """
    Return a merged column list:
      - existing columns keep their position
      - new columns are inserted following preferred_order,
        or appended alphabetically at the end if not in the preferred list
    """
    result = list(existing)
    truly_new = sorted(incoming - set(result))

    for col in preferred_order:
        if col not in truly_new:
            continue
        # Insert after the last preferred column already present
        anchors = [result.index(p) for p in preferred_order if p in result]
        insert_at = (max(anchors) + 1) if anchors else 0
        result.insert(insert_at, col)
        truly_new.remove(col)

    result.extend(truly_new)   # unknown new fields go to the far right
    return result


# ─────────────────────────── GOOGLE SHEETS ────────────────────

def open_worksheet(sheet_id: str, tab_name: str, log: logging.Logger) -> gspread.Worksheet:
    creds = Credentials.from_service_account_file(
        GOOGLE_SERVICE_ACCOUNT_JSON, scopes=SCOPES
    )
    gc = gspread.authorize(creds)
    spreadsheet = with_retry(gc.open_by_key, sheet_id, log=log, desc="open_by_key")
    try:
        ws = with_retry(spreadsheet.worksheet, tab_name, log=log, desc="worksheet")
        log.info("Opened tab '%s'.", tab_name)
    except gspread.WorksheetNotFound:
        log.info("Tab '%s' not found — creating it.", tab_name)
        ws = with_retry(
            spreadsheet.add_worksheet, title=tab_name, rows=1, cols=1, log=log, desc="add_worksheet"
        )
    return ws


def get_header(ws: gspread.Worksheet, log: logging.Logger) -> list[str]:
    try:
        return [c for c in with_retry(ws.row_values, 1, log=log, desc="row_values") if c]
    except Exception:
        return []


def ensure_header(
    ws: gspread.Worksheet,
    current: list[str],
    needed: set[str],
    preferred_order: list[str],
    log: logging.Logger,
) -> list[str]:
    """Extend the header row with any missing columns and return the updated list."""
    updated = merge_columns(current, needed, preferred_order)
    if updated != current:
        added = [c for c in updated if c not in current]
        log.info("New column(s) added to sheet: %s", added)
        with_retry(ws.update, "A1", [updated], log=log, desc="update header")
    return updated


def append_rows(ws: gspread.Worksheet, header: list[str], records: list[dict[str, str]], log: logging.Logger):
    rows = [[rec.get(col, "") for col in header] for rec in records]
    with_retry(ws.append_rows, rows, value_input_option="USER_ENTERED", log=log, desc="append_rows")
    log.info("Appended %d row(s).", len(rows))


# ─────────────────────────── MONGODB CURSOR ───────────────────

def get_cursor(db, daily_log_id: str, log: logging.Logger) -> datetime:
    """Read lastUpdateDatetime from DailyLog for the given _id."""
    doc = db["DailyLog"].find_one({"_id": daily_log_id})
    if doc and "lastUpdateDatetime" in doc:
        dt = doc["lastUpdateDatetime"]
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        log.info("Cursor: %s", dt.isoformat())
        return dt
    log.warning("DailyLog '%s' not found — syncing all records.", daily_log_id)
    return datetime.min.replace(tzinfo=timezone.utc)


def update_cursor(db, daily_log_id: str, dt: datetime, log: logging.Logger):
    """Write lastUpdateDatetime back to DailyLog."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    db["DailyLog"].update_one(
        {"_id": daily_log_id},
        {"$set": {"lastUpdateDatetime": dt}},
        upsert=True,
    )
    log.info("Cursor updated to %s", dt.isoformat())


# ─────────────────────────── BROKER ACCOUNTS ───────────────────

def fetch_broker_accounts(db, account_ids: set) -> dict[str, dict]:
    """Fetch BrokerAccount docs for the given ids, keyed by str(_id).
    Only `broker` and `name` are needed on the sheet.
    """
    account_ids = {a for a in account_ids if a}
    if not account_ids:
        return {}
    docs = db["BrokerAccount"].find(
        {"_id": {"$in": list(account_ids)}},
        {"broker": 1, "name": 1},
    )
    return {str(d["_id"]): d for d in docs}