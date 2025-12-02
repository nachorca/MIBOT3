from __future__ import annotations
from datetime import datetime, timedelta
from typing import List
import pytz

def dates_list(tz_name: str, start: str, end: str) -> List[str]:
    tz = pytz.timezone(tz_name)
    d0 = tz.localize(datetime.strptime(start, "%Y-%m-%d")).date()
    d1 = tz.localize(datetime.strptime(end, "%Y-%m-%d")).date()
    if d1 < d0:
        d0, d1 = d1, d0
    out, cur = [], d0
    while cur <= d1:
        out.append(cur.strftime("%Y-%m-%d"))
        cur += timedelta(days=1)
    return out

def last_ndays(tz_name: str, n: int) -> List[str]:
    tz = pytz.timezone(tz_name)
    today = datetime.now(tz).date()
    return [(today - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(0, max(1, n))]
