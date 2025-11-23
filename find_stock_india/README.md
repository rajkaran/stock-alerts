## Purpose
- **universe_fetch/** scripts: Download _lists of tickers_ (universe) for NSE/BSE ETFs, Stocks, Index Funds, and Mutual Funds and store in **MongoDB**. These scripts **do not** use yfinance.
- **analyzers/** scripts: Read tickers from MongoDB, fetch details from **yfinance**, evaluate your rules, and write result CSVs ranking items by how many rules they match.

## Setup
```bash
python -m venv venv && source venv/bin/activate
pip install pandas numpy pymongo yfinance requests python-dateutil
export MONGO_URL="mongodb://localhost:27017" # or your Atlas URI
export MONGO_DB="investment"
```

## Collections
- `universe_etfs` (symbol, name, exchange, source_fields)
- `universe_stocks` (symbol, name, exchange, series, source_fields)
- `universe_indexfunds` (scheme_code, name, amc, category, source_fields)
- `universe_mutualfunds` (scheme_code, name, amc, category, source_fields)

> Analyzers will write CSVs to `./output/` by default.

## Quick run commands (from repo root)
### activate venv if you use one
- `pip install -r requirements.txt`

### fetch universes into Mongo
```
python -m find_stock_india.universe_fetch.bootstrap_dhan_universe
python -m find_stock_india.universe_fetch.update_tickers_from_name --collection ExchangeTradedFunds
python -m find_stock_india.universe_fetch.update_tickers_from_name --collection IndexFunds
python -m find_stock_india.universe_fetch.update_tickers_from_name --collection Stocks
```

### analyze (writes CSVs to ./output/)
```
python -m find_stock_india.analyzers.analyze_etfs
python -m find_stock_india.analyzers.analyze_stocks
python -m find_stock_india.analyzers.analyze_indexfunds
python -m find_stock_india.analyzers.analyze_mutualfunds
```