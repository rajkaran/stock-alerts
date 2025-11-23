from __future__ import annotations

import os
import logging
from datetime import datetime, date, time, timedelta, timezone
from typing import Dict, List, Any

from dotenv import load_dotenv
from zoneinfo import ZoneInfo
from pymongo import MongoClient, ASCENDING
import smtplib
from email.message import EmailMessage

load_dotenv()

# ------------------- CONFIG -------------------

TZ = ZoneInfo(os.getenv("TZ", "America/Toronto"))

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.getenv("MONGO_DB", "stockdb")

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("email_canadian_signals")

# Email config (set these in your .env)
SMTP_HOST = os.getenv("SMTP_HOST", "")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))  # default TLS port
SMTP_USER = os.getenv("EMAIL_USER", "")
SMTP_PASSWORD = os.getenv("EMAIL_PASS", "")
EMAIL_FROM = os.getenv("EMAIL_FROM", "no-reply@iamraj.com")
EMAIL_SUBJECT = os.getenv("EMAIL_SUBJECT", "Tickers can be invested in - ")

# ------------------- MONGO --------------------

client = MongoClient(MONGO_URI)
db = client[MONGO_DB]

exec_state_col = db["EveryExecutionState"]
notify_email_col = db["NotifyEmail"]
email_log_col = db["EmailLog"]

# Optional, but helpful if you query logs later
email_log_col.create_index([("createDatetime", ASCENDING)])


# ------------------- HELPERS --------------------

def get_today_utc_range():
    """Return (start_utc, end_utc) for 'today' in local TZ."""
    now_local = datetime.now(TZ)
    today = now_local.date()

    start_local = datetime.combine(today, time(0, 0, 0), tzinfo=TZ)
    end_local = start_local + timedelta(days=1)

    start_utc = start_local.astimezone(timezone.utc)
    end_utc = end_local.astimezone(timezone.utc)

    return start_utc, end_utc


def fetch_today_execution_docs():
    """Fetch all EveryExecutionState documents created 'today' (local)."""
    start_utc, end_utc = get_today_utc_range()

    log.info("Querying EveryExecutionState from %s to %s (UTC)", start_utc, end_utc)

    cursor = exec_state_col.find(
        {
            "createDatetime": {
                "$gte": start_utc,
                "$lt": end_utc,
            }
        }
    )

    docs = list(cursor)
    log.info("Found %d docs till %s", len(docs), end_utc)
    return docs


def aggregate_matches(docs: List[dict]) -> List[Dict[str, Any]]:
    """
    From today's execution docs, collect all matches for these fields:
      - lessThanMin90
      - lessThan80PctDiff90
      - lessThanMin30

    For each (field, ticker) pair, keep the minimum price across all docs
    and its corresponding compareWith value.

    Returns: list of rows:
      { "ticker": str, "field": str, "minPrice": float, "compareWith": float }
    """
    target_fields = [
        "lessThanMin90",
        "lessThan80PctDiff90",
        "lessThanMin30",
    ]

    # data[field][ticker] = {"minPrice": x, "compareWith": y}
    data: Dict[str, Dict[str, Dict[str, Any]]] = {f: {} for f in target_fields}

    for doc in docs:
        for field in target_fields:
            entries = doc.get(field, [])
            if not isinstance(entries, list):
                continue

            for entry in entries:
                ticker = entry.get("ticker")
                price = entry.get("price")
                compare_with = entry.get("compareWith")

                if ticker is None or price is None:
                    continue

                price = float(price)
                compare_with = float(compare_with) if compare_with is not None else None

                bucket = data[field].setdefault(
                    ticker,
                    {"minPrice": price, "compareWith": compare_with},
                )

                # If we've seen this ticker+field before, keep the lowest price
                if price < bucket["minPrice"]:
                    bucket["minPrice"] = price
                    bucket["compareWith"] = compare_with
    
    # Flatten into a list of rows
    rows: List[Dict[str, Any]] = []
    for field, by_ticker in data.items():
        for ticker, info in by_ticker.items():
            rows.append(
                {
                    "ticker": ticker,
                    "field": field,
                    "minPrice": info["minPrice"],
                    "compareWith": info["compareWith"],
                }
            )
   
    # Optionally sort by field then ticker
    rows.sort(key=lambda r: (r["field"], r["ticker"]))
    log.info("Aggregated %d (field, ticker) rows", len(rows))
    return rows


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
        return "No matching signals for today."

    lines = []
    lines.append("Ticker / Field / MinPrice / CompareWith")
    lines.append("--------------------------------------------------")
    for r in rows:
        cw = r["compareWith"]
        cw_str = "N/A" if cw is None else f"{cw:.4f}"
        lines.append(
            f"{r['ticker']} | {r['field']} | "
            f"{r['minPrice']:.4f} | {cw_str}"
        )

    return "\n".join(lines)


def pretty_field_name(field: str) -> str:
    """Optional: Convert internal field names to nicer labels."""
    mapping = {
        "lessThanMin90": "Less than minimum of last 90 days",
        "lessThan80PctDiff90": "80th percent of difference between average and minimum of previous 90 days",
        "lessThanMin30": "Less than minimum of last 90 days",
    }
    return mapping.get(field, field)


def format_table_html(rows: List[Dict[str, Any]]) -> str:
    """Format the aggregated rows into an HTML table for the email body."""
    if not rows:
        # Simple HTML for no results
        return """
        <html>
          <body style="font-family: Arial, sans-serif; font-size: 14px;">
            <p>No matching signals for today.</p>
          </body>
        </html>
        """

    # Build table rows
    row_html_parts = []
    for r in rows:
        cw = r["compareWith"]
        cw_str = "N/A" if cw is None else f"{cw:.4f}"

        row_html_parts.append(f"""
          <tr>
            <td style="border:1px solid #ccc; padding:4px 8px;">{r['ticker']}</td>
            <td style="border:1px solid #ccc; padding:4px 8px;">{pretty_field_name(r['field'])}</td>
            <td style="border:1px solid #ccc; padding:4px 8px; text-align:right;">{r['minPrice']:.4f}</td>
            <td style="border:1px solid #ccc; padding:4px 8px; text-align:right;">{cw_str}</td>
          </tr>
        """)

    rows_html = "\n".join(row_html_parts)

    # Full HTML document
    html = f"""
    <html>
      <body style="font-family: Arial, sans-serif; font-size: 14px;">
        <p>Signals for today:</p>
        <table style="border-collapse: collapse; border:1px solid #ccc;">
          <thead>
            <tr style="background-color:#f2f2f2;">
              <th style="border:1px solid #ccc; padding:4px 8px; text-align:left;">Ticker</th>
              <th style="border:1px solid #ccc; padding:4px 8px; text-align:left;">Condition</th>
              <th style="border:1px solid #ccc; padding:4px 8px; text-align:right;">Min Price</th>
              <th style="border:1px solid #ccc; padding:4px 8px; text-align:right;">Compare With</th>
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


def log_email_send(recipients: List[str], subject: str, row_count: int):
    """Insert a log entry into EmailLog."""
    now_utc = datetime.now(timezone.utc)
    doc = {
        "createDatetime": now_utc,
        "subject": subject,
        "recipients": recipients,
        "rowCount": row_count,
    }
    email_log_col.insert_one(doc)
    log.info("EmailLog entry created (%d rows)", row_count)


# ------------------- MAIN --------------------

def main():
    # 1) Fetch today's execution docs
    docs = fetch_today_execution_docs()

    # 2) Aggregate matches for the 3 fields
    rows = aggregate_matches(docs)
    print(rows)

    # 3) Format table for email
    body_text = format_table_text(rows)
    body_html = format_table_html(rows)

    # 4) Fetch recipients
    recipients = get_notify_emails()

    # 5) Send email
    sent = send_email(recipients, EMAIL_SUBJECT, body_text, body_html)

    # 6) Log the email attempt if sent
    if sent:
        log_email_send(recipients, EMAIL_SUBJECT, len(rows))


if __name__ == "__main__":
    main()
