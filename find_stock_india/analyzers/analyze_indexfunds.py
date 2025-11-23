"""Stub: Read index funds from Mongo, evaluate MF rules (HDFC NRE availability, balance).
You can extend this to fetch portfolio data via MF APIs if you add them later.
"""
import os
import pandas as pd
from common.mongo_utils import col


OUT = os.getenv("OUT_INDEXFUNDS", "output/indexfund_results.csv")

def main():
    funds = list(col("universe_indexfunds").find({}, {"_id":0}))
    df = pd.DataFrame(funds)
    # For now just dump universe; enrichment can be added later.
    os.makedirs("output", exist_ok=True)
    df.to_csv(OUT, index=False)
    print(f"Saved {len(df)} index fund rows to {OUT}")


if __name__ == "__main__":
    main()