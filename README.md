# Stock Alerts (Canadian Stocks)

This is a small Python batch application that fetches **Canadian stock data** (via the [Finnhub API](https://finnhub.io)) and checks for alert conditions based on the last 30 and 90 days of prices.

## What it does

- Fetches daily price candles for configured tickers
- Calculates:
  - Average closing price over the last 30 and 90 days
  - Lowest price over the last 30 and 90 days
- Triggers alerts when the latest price falls below:
  - The average of the last 30/90 days
  - 50% between average and lowest (30/90 days)
  - 80% between average and lowest (30/90 days)
  - The minimum price of the last 30/90 days

Currently, alerts are printed to the console.  
Future versions will add **email notifications** and scheduling.

---

## Setup & Installation

### 1. Clone the repository

```bash
git clone https://github.com/your-username/stock_alerts.git
cd stock_alerts
```

### 2. Create a virtual environment
```bash
python3 -m venv venv
```
Activate with `source venv/bin/activate`.

### 3. Install dependencies
```bash
pip install -r requirements.txt
```

### 4. Add your Finnhub API key
Create a .env file (not committed to git) and add:
```bash
FINNHUB_API_KEY=your_api_key_here
```
Alternatively, you can paste the key directly into alerts.py (not recommended).

### 5. Run the script
```bash
python3 ./fetch_historical_data.py
```