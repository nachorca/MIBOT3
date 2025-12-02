# tools/audit_sicu_libya.py
# -*- coding: utf-8 -*-
from __future__ import annotations
from pathlib import Path
from typing import Dict, List, Tuple
import csv

BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "data"
OUTPUT_DIR = BASE_DIR / "output"
GAZETTEER_DIR = DATA_DIR / "gazetteer"

# === helpers numéricos ===
def _to_float(val: str | None) -> float | None:
    if val is None:
        return None
    s = str(val).strip()
    if not s:
        return None
    if "," in s and "." not in s:
        s = s.replace(",", ".")
    try:
        return float(s)
    except Exception:
        return None

# === cargar gazetteer libia con cabeceras robustas ===
def load_gazetteer_libya() -> Tuple[List[Dict[str,str]], Dict[str,str]]:
    gfile = GAZETTEER_DIR / "libia.csv"
    rows: List[Dict[str,str]] = []
    colmap: Dict[str,str] = {}
    with gfile.open(newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for h in r.fieldnames or []:
            key = (h or "").strip()
            low = key.lower()
            if low in {"name","nombre","localidad","city","town"}:
                colmap["name"] = key
            elif low in {"aliases","alias"}:
                colmap["aliases"] = key
            elif low in {"lat","latitude","y"}:
                colmap["lat"] = key
            elif low in {"lon","long","lng","longitude","x"}:
                colmap["lon"] = key
        for row in r:
            rows.append(row)
    return rows, colmap

def row_get(row: Dict[str,str], colmap: Dict[str,str], key: str) -> str:
    real = colmap.get(key)
    if not real:
        return ""
    return (row.get(real) or "").strip()

def lookup_loc(loc: str, gzt: List[Dict[str,str]], colmap: Dict[str,str]) -> Tuple[float|None,float|None,str]:
    # prueba segmentos de la localización: "Ain Zara, Tripoli, Libya" -> "Ain Zara", "Tripoli", "Libya"
    segments = [s.strip() for s in loc.split(",") if s.strip()]
    best_lat = best_lon = None
    best_name = ""
    for seg in segments:
        search = seg.lower()
        for row in gzt:
            name = row_get(row,colmap,"name")
            aliases = row_get(row,colmap,"aliases").split("|")
            for cand in [name] + aliases:
                c = cand.strip()
                if not c:
                    continue
                if c.lower() == search:
                    try:
                        lat = float(row_get(row,colmap,"lat"))
                        lon = float(row_get(row,colmap,"lon"))
                    except Exception:
                        continue
                    return lat, lon, name or c
    return None, None, ""

def lookup_desc(desc: str, gzt: List[Dict[str,str]], colmap: Dict[str,str]) -> Tuple[float|None,float|None,str]:
    text = (desc or "").lower()
    best_lat = best_lon = None
    best_name = ""
    for row in gzt:
        name = row_get(row,colmap,"name")
        aliases = row_get(row,colmap,"aliases").split("|")
        for cand in [name] + aliases:
            c = cand.strip()
            if not c:
                continue
            token = c.lower()
            if token and token in text:
                try:
                    lat = float(row_get(row,colmap,"lat"))
                    lon = float(row_get(row,colmap,"lon"))
                except Exception:
                    continue
                return lat, lon, name or c
    return None, None, ""

def audit_csv_sicu_libya(date_str: str) -> None:
    gzt, colmap = load_gazetteer_libya()
    csv_path = OUTPUT_DIR / "incidentes_categorizados" / "libia" / f"libia-{date_str}_incidentes_SICU.csv"
    if not csv_path.exists():
        print(f"❌ No existe CSV SICU: {csv_path}")
        return

    with csv_path.open(newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        rows = list(r)

    total = len(rows)
    unresolved: List[Dict[str,str]] = []
    resolved = 0

    for row in rows:
        loc = (row.get("Localización") or row.get("localizacion") or "").strip()
        desc = (row.get("Breve descripción") or row.get("descripcion") or "").strip()
        lat = _to_float(row.get("Lat") or row.get("lat"))
        lon = _to_float(row.get("Lon") or row.get("lon"))

        if lat is not None and lon is not None:
            resolved += 1
            continue

        lat1, lon1, name1 = lookup_loc(loc, gzt, colmap)
        if lat1 is not None and lon1 is not None:
            resolved += 1
            continue

        lat2, lon2, name2 = lookup_desc(desc, gzt, colmap)
        if lat2 is not None and lon2 is not None:
            resolved += 1
            continue

        unresolved.append({
            "categoria": row.get("Categoría SICU") or row.get("categoria_sicu") or "",
            "fecha": row.get("Fecha") or row.get("fecha") or "",
            "hora": row.get("Hora") or row.get("hora") or "",
            "loc": loc,
            "extracto": desc[:140].replace("\n"," "),
        })

    print(f"📊 LIBIA {date_str} – filas totales: {total}, resueltas (con coords o gazetteer): {resolved}, SIN coordenadas: {len(unresolved)}")
    if unresolved:
        print("🔎 Incidentes SIN coordenadas (localización/descripcion no casan con el gazetteer):")
        for i, r in enumerate(unresolved, start=1):
            print(f"{i:02d}. [{r['fecha']} {r['hora']}] {r['categoria']}")
            print(f"    Loc: {r['loc']}")
            print(f"    Desc: {r['extracto']}")
    else:
        print("✅ Todos los incidentes de este día se pudieron geolocalizar.")

if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("Uso: python tools/audit_sicu_libya.py YYYY-MM-DD")
        sys.exit(1)
    audit_csv_sicu_libya(sys.argv[1])