from __future__ import annotations
from pathlib import Path
from datetime import datetime
from typing import Iterable, Tuple
import re

DT_RE = re.compile(r"^--- .* @ (\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) ---\s*$")

def _parse_entries(text: str):
    """
    Generador de (dt: datetime|None, chunk: str). Separa por cabeceras '--- ... @ DT ---'.
    """
    buf = []
    current_dt = None
    lines = text.splitlines(keepends=True)
    for line in lines:
        m = DT_RE.match(line)
        if m:
            # yield anterior (si hay)
            if buf:
                yield current_dt, "".join(buf)
                buf = []
            try:
                current_dt = datetime.strptime(m.group(1), "%Y-%m-%d %H:%M:%S")
            except Exception:
                current_dt = None
            buf.append(line)
        else:
            buf.append(line)
    if buf:
        yield current_dt, "".join(buf)

def read_country_window(
    data_dir: str,
    country: str,
    start_dt: datetime,
    end_dt: datetime,
) -> str:
    """
    Lee ficheros diarios necesarios y devuelve solo las entradas con dt en [start_dt, end_dt).
    Ten en cuenta que tus TXT son por fecha civil. Para cubrir 07:00–06:59 hay que leer 1 o 2 archivos.
    """
    country_dir = Path(data_dir) / country.lower()
    country_dir.mkdir(parents=True, exist_ok=True)

    # Archivos a leer: el día civil de start_dt y el día civil de end_dt-1s
    dates = sorted(
        {start_dt.strftime("%Y-%m-%d"), (end_dt).astimezone(start_dt.tzinfo).strftime("%Y-%m-%d")}
    )
    chunks = []

    for day in dates:
        f = country_dir / f"{day}.txt"
        if not f.exists():
            continue
        text = f.read_text(encoding="utf-8")
        for dt, block in _parse_entries(text):
            # Si no pudimos parsear dt, lo dejamos pasar (conservador) o descartamos; preferimos descartar
            if dt is None:
                continue
            # important: dt es naive? En tus entradas es "local" sin tz. Comparamos como naive en misma zona.
            if start_dt.replace(tzinfo=None) <= dt <= end_dt.replace(tzinfo=None):
                chunks.append(block)

    if not chunks:
        return ""
    header = f"===== {country.upper()} :: {start_dt.strftime('%Y-%m-%d %H:%M')} → {end_dt.strftime('%Y-%m-%d %H:%M')} =====\n"
    return header + "".join(chunks) + "\n"