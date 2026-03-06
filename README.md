

# **Stock Alerts – Canadian Tickers**

This project ingests 5-minute price data for selected Canadian tickers from Yahoo Finance, stores it in MongoDB, runs rolling analytics, and emails you “interesting” tickers during market hours.

Core scripts:



* `fetch_historical_data.py` – **one-time (or occasional) bootstrap** of 5-minute history. \

* `weekly_canadian_ticker.py` – **continuous ingestion + analytics** (runs every 2 minutes between 9-15 on weekdays). \


---


## **1. Data Model & Collections**

MongoDB database (default): `stockdb`

Collections:


```


### PriceFor5MinuteInterval
```


Raw 5-minute OHLCV bars from Yahoo Finance.

Each document (per 5-minute bar, per ticker):


```
{
  "ticker": "BCE.TO",
  "ts": ISODate("2025-08-28T13:30:00Z"),  // UTC datetime
  "Open": 34.31,
  "High": 34.34,
  "Low": 34.02,
  "Close": 34.04,
  "Adj Close": 34.04,
  "Volume": 12345,
  "createDatetime": ISODate("2025-11-22T19:25:30Z")  // when written
}
```



    `ts` is the primary time key; `createDatetime` is when this bar was stored.


---


```


### DailyLog
```


Singleton collection to track when we last fetched **yesterday’s 5-minute data**.


```
{
  "_id": "singleton",
  "lastUpdateDatetime": ISODate("2025-11-23T03:00:00Z")
}
```


Used by `weekly_canadian_ticker.py` to avoid fetching the same day multiple times.


---


```


### EveryExecutionState
```


Snapshot of **analysis results** for a single run of `weekly_canadian_ticker.py`.

Each document:


```
{
  "_id": ObjectId(...),
  "createDatetime": ISODate("2025-11-23T15:00:00Z"),
  "lessThanAvg30": [
    { "ticker": "BCE.TO", "price": 44.32, "compareWith": 46.10 },
    ...
  ],
  "lessThanAvg90": [ ... ],
    ...
  "lessThanMin30": [ ... ],
  "lessThanMin90": [ ... ],

  "lessThan80PctDiff30": [
    { "ticker": "T.TO", "price": 18.71, "compareWith": 19.21 }
  ],
  "lessThan50PctDiff30": [ ... ],
  "lessThan80PctDiff90": [ ... ],
  "lessThan50PctDiff90": [ ... ]
}
```


Each array is:



* one **comparison rule \
**
* containing objects with: \

    * `ticker \
`
    * `price` (current price during this run) \

    * `compareWith` (threshold used for that rule) \



---


```


### NotifyEmail
```


Where the system reads **recipients** for email alerts.

Example documents:


```
{ "_id": ObjectId(...), "email": "rajkaran.chauhan07@hotmail.com", "isActive": true }
{ "_id": ObjectId(...), "email": "rk.chauhan@hotmail.com", "isActive": true }
```


You can also support:


```
{ "_id": ObjectId(...), "emails": ["you@example.com", "me@example.com"] }


---


### EmailLog
```


Log of each email send attempt that succeeded:


```
{
  "_id": ObjectId(...),
  "createDatetime": ISODate("2025-11-23T15:30:01Z"),
  "subject": "Tickers can be invested in - ",
  "recipients": [
    "rajkaran.chauhan07@hotmail.com",
    "rk.chauhan@hotmail.com"
  ],
  "rowCount": 3
}


---
```



## **2. Environment Variables (<code>.env</code>)**

Project uses[ python-dotenv](https://pypi.org/project/python-dotenv/) to load config from a `.env` file in the project root.

Example `.env`:


```
# Timezone
TZ=America/Toronto

# Mongo
MONGO_URI=mongodb://localhost:27017
MONGO_DB=stockdb

# Tickers to track (comma-separated)
TICKERS=BCE.TO,BNS.TO,CM.TO,CSH-UN.TO,ENB.TO,FIE.TO,FTS.TO,POW.TO,SGR-UN.TO,SRU-UN.TO,T.TO,TD.TO

# Email / SMTP
SMTP_HOST=smtp.yourprovider.com
SMTP_PORT=587

# SMTP login
EMAIL_USER=your_smtp_username@example.com
EMAIL_PASS=your_smtp_password

# From / subject
EMAIL_FROM=your_smtp_username@example.com
EMAIL_SUBJECT=Tickers can be invested in -
```



    For deliverability (especially Hotmail), it’s best if `EMAIL_FROM` matches your authenticated `EMAIL_USER` and that domain has correct SPF/DKIM.


---


## **3. Script Overview**


### **3.1 <code>fetch_historical_data.py</code> (Bootstrap 5-minute history)**

**Purpose:** one-time or manual bootstrap to pull **last 60 days of 5-minute data** for configured tickers and write them into `PriceFor5MinuteInterval`.

Key points:



* Uses `yfinance` with `interval="5m"` and `period="60d"`. \

* Flattens yfinance’s MultiIndex columns into simple `Open`, `High`, `Low`, `Close`, `Adj Close`, `Volume`. \

* Stores each 5-minute bar with: \

    * `ticker \
`
    * `ts` (UTC datetime from index) \

    * OHLCV columns \

    * `createDatetime` (when this row was written) \


Usage (manual):


```
cd /home/rajkaran/projects/stock_alerts
source venv/bin/activate
python fetch_historical_data.py


---
```



### **3.2 <code>analyze_canadian_ticker.py</code> (Ingestion + analytics, runs every 2 minutes)**

**Responsibilities:**



1. **Ensure yesterday’s 5-minute data is loaded** into `PriceFor5MinuteInterval` (once per day). \

    * Reads `DailyLog._id="singleton"`’s `lastUpdateDatetime`. \

    * If last update date != today (in `TZ`), downloads **yesterday’s** 5-minute data for each ticker and upserts into `PriceFor5MinuteInterval`, then updates `DailyLog`. \

4. **Run comparison rules** per ticker and write into `EveryExecutionState`: \
 \
 Calculate if current price of a ticker is less that 1-14 weeks period. whatever number of week is less than goes to the final object tobe notified to user.


For each rule, if a ticker passes it, an object is appended: \
 \
 `{`


```
  "ticker": "T.TO",
  "price": 18.71,
  "compareWith": 19.21
}

```



5. 

**Usage (manual):**


```
cd /home/rajkaran/projects/stock_alerts
source venv/bin/activate
python analyze_canadian_ticker.py


---
```



### **3.3 <code>email_canadian_signals.py</code> (Aggregation + email, runs 3× per trading day)**

**Responsibilities:**



1. **Fetch all <code>EveryExecutionState</code> documents created “today”** (using local TZ → UTC window). \
 
5. **Format email**: \

    * Plain text fallback (simple pipe-separated lines). \




    * 
HTML email with a clean table: \


<table>
  <tr>
   <td><strong>Ticker</strong>
   </td>
   <td><strong>Condition</strong>
   </td>
   <td><strong>Min Price</strong>
   </td>
   <td><strong>Compare With</strong>
   </td>
  </tr>
  <tr>
   <td>T.TO
   </td>
   <td>&lt; 80% band of 90-day range
   </td>
   <td>18.7100
   </td>
   <td>19.2100
   </td>
  </tr>
</table>




    *  \

6. **Fetch recipients from <code>NotifyEmail</code>**: \

    * Accepts `email` field (string), and/or `emails` field (array of strings). \

    * Deduplicates addresses. \

7. **Send email using SMTP**: \

    * Host/port from `.env` (`SMTP_HOST`, `SMTP_PORT`). \

    * Auth using `EMAIL_USER`/`EMAIL_PASS`. \

    * `From` set to `EMAIL_FROM`. \

    * `Subject` set to `EMAIL_SUBJECT`. \

8. **Log the send to <code>EmailLog</code>** (if the send succeeded). \


**Usage (manual):**


```
cd /home/rajkaran/projects/stock_alerts
source venv/bin/activate
python email_canadian_signals.py
python3 ./weekly_canadian_ticker.py 


---
```



## **4. Local Setup**


### **4.1 Clone & create venv**


```
cd /home/rajkaran/projects
git clone <your-repo-url> stock_alerts
cd stock_alerts

python3 -m venv venv
source venv/bin/activate
```



### **4.2 Install dependencies**

Typical `requirements.txt` (adjust if needed):


```
pymongo
yfinance
pandas
python-dotenv
```


Install:


```
pip install -r requirements.txt
```



### **4.3 Configure <code>.env</code></strong>

Create `.env` in the project root with:



* Mongo config \

* TZ \

* TICKERS \

* SMTP config \


(See example above.)


---


## **5. Running Scripts Locally**

From project root:


```
cd /home/rajkaran/projects/stock_alerts
source venv/bin/activate
```


**Bootstrap historical 5-minute data (optional, one-time): \
 \
** `python fetch_historical_data.py`



1. 

**Run analytics once: \
 \
** `python analyze_canadian_ticker.py`



2. 

**Send current signals email once: \
 \
** `python email_canadian_signals.py`



3. 

Watch logs (if you’re logging to `logs/run.log` or per-script logs).


---


## **6. Cron Setup (Ubuntu)**

We’re using:



* `analyze_canadian_ticker.py` → every **2 minutes**, Mon–Fri (trading days). \

* `email_canadian_signals.py` → at **10:30**, **12:30**, **14:30**, Mon–Fri. \


Assuming:



* Project root: `/home/rajkaran/projects/stock_alerts \
`
* Venv: `/home/rajkaran/projects/stock_alerts/venv \
`
* Logs directory: `/home/rajkaran/projects/stock_alerts/logs \
`


### **6.1 Ensure <code>logs</code> directory exists**


```
cd /home/rajkaran/projects/stock_alerts
mkdir -p logs
```



### **6.2 Edit crontab**


```
crontab -e
```


Pick `nano` if asked.

Add:


```
# Analyze Canadian tickers every 2 minutes on trading days (Mon-Fri)
*/2 * * * 1-5 cd /home/rajkaran/projects/stock_alerts && /home/rajkaran/projects/stock_alerts/venv/bin/python analyze_canadian_ticker.py >> /home/rajkaran/projects/stock_alerts/logs/run.log 2>&1

# Email Canadian signals at 10:30, 12:30, and 14:30 on trading days (Mon-Fri)
30 10 * * 1-5 cd /home/rajkaran/projects/stock_alerts && /home/rajkaran/projects/stock_alerts/venv/bin/python email_canadian_signals.py >> /home/rajkaran/projects/stock_alerts/logs/run.log 2>&1
30 12 * * 1-5 cd /home/rajkaran/projects/stock_alerts && /home/rajkaran/projects/stock_alerts/venv/bin/python email_canadian_signals.py >> /home/rajkaran/projects/stock_alerts/logs/run.log 2>&1
30 14 * * 1-5 cd /home/rajkaran/projects/stock_alerts && /home/rajkaran/projects/stock_alerts/venv/bin/python email_canadian_signals.py >> /home/rajkaran/projects/stock_alerts/logs/run.log 2>&1
```


Save + exit.

Check:


```
crontab -l
```


You should see the same entries.


### **6.3 Verify logs**

After cron has run for a bit:


```
cd /home/rajkaran/projects/stock_alerts
tail -n 100 logs/run.log
```


You should see:



* `DailyLog` messages \

* Stats + current price logs \

* `Email sent to: ... \
`
* `EmailLog entry created (N rows) \
`


---


## **7. Notes / Gotchas**



* **Timezone:** logic uses `TZ=America/Toronto` for: \

    * interpreting “today” \

    * fetching **yesterday’s** 5-minute data \

* **Yahoo Finance limits:** you’re using: \

    * `5m` with `period="60d"` for history \

    * `1m` with `period="1d"` for current price \
 If Yahoo ever changes their limits, these might need adjusting. \

* **Hotmail deliverability: \
** Hotmail can be picky. To help: \

    * Use `EMAIL_FROM` that matches `EMAIL_USER` (same address). \

    * Use a “normal-looking” subject and content (you already do). \

    * Consider adding SPF/DKIM for your sending domain if you use a custom domain.