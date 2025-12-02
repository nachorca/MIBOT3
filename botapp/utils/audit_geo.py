# -*- coding: utf-8 -*-
from __future__ import annotations
from pathlib import Path
import csv
from typing import Tuple

def _to_float(s):
    if s is None: return None
    t = str(s).strip()
    if not t: return None
    if "," in t and "." not in t: t = t.replace(",", ".")
    try: return float(t)
    except: return None

def audit_csv(csv_path: str | Path) -> Tuple[int,int,int,list, list]:
    """
    Devuelve: (total, validos, sin_coord, ejemplos_sin_coord[<=5], ejemplos_coord_malas[<=5])
    """
    p = Path(csv_path)
    with open(p, newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        rows = list(r)
    total = len(rows)
    valid = 0
    no_coord = []
    bad = []  # coords no parseables
    for i, row in enumerate(rows, 1):
        lat = _to_float(row.get("Lat")); lon = _to_float(row.get("Lon"))
        if lat is None or lon is None:
            if len(no_coord) < 5:
                no_coord.append((i, row.get("Localización"), row.get("Breve descripción","")[:80]))
            continue
        try:
            float(lat); float(lon)
            valid += 1
        except:
            if len(bad) < 5:
                bad.append((i, row.get("Lat"), row.get("Lon")))
    return total, valid, (total - valid), no_coord, bad