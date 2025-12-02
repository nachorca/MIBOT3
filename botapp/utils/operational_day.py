from __future__ import annotations
from datetime import datetime, timedelta
from typing import Tuple, List
import pytz

def opday_bounds(tz_name: str, day_str: str) -> Tuple[datetime, datetime]:
    """
    Devuelve ventana [start, end) del 'día operativo' (07:00 → 06:59).
    day_str: YYYY-MM-DD (fecha civil del comienzo del op-day).
    start = YYYY-MM-DD 07:00:00 (local)
    end   = (YYYY-MM-DD + 1) 07:00:00 (local)
    """
    tz = pytz.timezone(tz_name)
    d = datetime.strptime(day_str, "%Y-%m-%d")
    start = tz.localize(d.replace(hour=7, minute=0, second=0, microsecond=0))
    end = start + timedelta(days=1)
    return start, end

def opday_list(tz_name: str, start_str: str, end_str: str) -> List[str]:
    """
    Lista de fechas YYYY-MM-DD (inclusive) entre start_str y end_str,
    cada una representa el inicio del día operativo de ese día a las 07:00.
    """
    d0 = datetime.strptime(start_str, "%Y-%m-%d").date()
    d1 = datetime.strptime(end_str, "%Y-%m-%d").date()
    if d1 < d0:
        d0, d1 = d1, d0
    out: List[str] = []
    cur = d0
    while cur <= d1:
        out.append(cur.strftime("%Y-%m-%d"))
        cur += timedelta(days=1)
    return out

def last_n_opdays(tz_name: str, n: int) -> List[str]:
    """
    Últimos n 'días operativos' terminando en el op-day que comienza hoy a las 07:00 (local).
    """
    tz = pytz.timezone(tz_name)
    now = datetime.now(tz)
    today_local_date = now.date()
    return [(today_local_date - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(0, max(1, n))]

def opday_for_local_dt(tz_name: str, local_dt: datetime) -> str:
    """
    Devuelve YYYY-MM-DD del op-day al que pertenece local_dt (07:00 → 06:59).
    """
    if local_dt.tzinfo is None:
        dt_loc = local_dt
    else:
        dt_loc = local_dt.astimezone(pytz.timezone(tz_name))
    if dt_loc.hour < 7:
        dt_loc = (dt_loc - timedelta(days=1))
    return dt_loc.strftime("%Y-%m-%d")

def opday_for_utc_dt(tz_name: str, utc_dt: datetime) -> str:
    """
    Igual que arriba, pero partiendo de un datetime en UTC (aware/naive).
    """
    tz = pytz.timezone(tz_name)
    if utc_dt.tzinfo is None:
        utc_dt = utc_dt.replace(tzinfo=pytz.utc)
    local_dt = utc_dt.astimezone(tz)
    return opday_for_local_dt(tz_name, local_dt)

def opday_today_str(tz_name: str) -> str:
    """
    YYYY-MM-DD del op-day correspondiente a AHORA en la TZ dada.
    """
    tz = pytz.timezone(tz_name)
    now_local = datetime.now(tz)
    return opday_for_local_dt(tz_name, now_local)