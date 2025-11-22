"""Stub: Read all mutual funds from Mongo. To apply your rules (HDFC NRE + balance),
add columns to the universe or join an external CSV with availability & allocation.
"""
import os
import pandas as pd
from common.mongo_utils import col


OUT = os.getenv("OUT_MFUNDS", "output/mfund_results.csv")

def main():
    funds = list(col("universe_mutualfunds").find({}, {"_id":0}))
    df = pd.DataFrame(funds)
    os.makedirs("output", exist_ok=True)
    df.to_csv(OUT, index=False)
    print(f"Saved {len(df)} mutual fund rows to {OUT}")


if __name__ == "__main__":
    main()