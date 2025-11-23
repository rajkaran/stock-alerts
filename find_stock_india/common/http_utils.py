import requests
import pandas as pd
from io import StringIO


UA = {
"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome Safari",
"Accept": "text/csv,application/csv,application/json,application/vnd.ms-excel,*/*",
"Referer": "https://www.nseindia.com/",
}


def get_csv(url: str, timeout: int = 25) -> pd.DataFrame:
    r = requests.get(url, headers=UA, timeout=timeout)
    r.raise_for_status()
    return pd.read_csv(StringIO(r.text))