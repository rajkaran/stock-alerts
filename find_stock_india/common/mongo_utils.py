import os
from typing import Any, Dict
from pymongo import MongoClient, ReplaceOne
from dotenv import load_dotenv

load_dotenv()  # load .env if present

MONGO_URL = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.getenv("MONGO_DB", "stockdb")


_client = MongoClient(MONGO_URL)
_db = _client[MONGO_DB]


def col(name: str):
    return _db[name]


def upsert_many_by_key(coll_name: str, docs, key: str):
    if not docs:
        return 0
    ops = [ReplaceOne({key: d[key]}, d, upsert=True) for d in docs if key in d]
    res = col(coll_name).bulk_write(ops, ordered=False)
    return res.upserted_count + res.modified_count