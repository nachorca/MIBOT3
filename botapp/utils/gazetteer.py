# -*- coding: utf-8 -*-
"""
Módulo de soporte de GEOLOCALIZACIÓN:
- Carga el archivo CSV del gazetteer para cada país.
- Ofrece función para buscar la mejor coincidencia de localización
  a partir de un texto de descripción del incidente.
"""

from __future__ import annotations
from pathlib import Path
import csv
import unicodedata
import re
from typing import Optional, Tuple, List, Dict

from botapp.config import get_settings

SET = get_settings()
DATA_DIR = Path(SET.data_dir).resolve()
GAZETTEER_DIR = DATA_DIR / "gazetteer"

def _norm(s: str) -> str:
    """Normaliza texto: elimina tildes, pasa a minúscula, sin espacios repetidos."""
    if not s:
        return ""
    s = unicodedata.normalize("NFD", s)
    s = "".join(c for c in s if unicodedata.category(c) != "Mn")
    s = s.lower()
    s = re.sub(r"\s+", " ", s).strip()
    return s

def load_gazetteer(country_slug: str) -> List[Dict[str, str]]:
    """Carga el csv del gazetteer para el país dado (slug) y devuelve lista de filas."""
    path = GAZETTEER_DIR / f"{country_slug}.csv"
    if not path.exists():
        return []
    rows: List[Dict[str, str]] = []
    with path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r in reader:
            rows.append(r)
    return rows

def match_location(text: str, gazetteer_rows: List[Dict[str, str]]) -> Optional[Tuple[str, str, str]]:
    """Intenta emparejar un texto de descripción con alguna localidad del gazetteer.
    Devuelve (name, lat, lon) si encuentra coincidencia; si no, None."""
    if not text or not gazetteer_rows:
        return None
    norm_text = _norm(text)
    words = set(re.findall(r"\w+", norm_text))
    for row in gazetteer_rows:
        name = (row.get("name") or "").strip()
        aliases_raw = row.get("aliases") or ""
        aliases = [a.strip() for a in aliases_raw.split("|") if a.strip()]
        candidates = [name] + aliases
        for cand in candidates:
            token = _norm(cand)
            if not token:
                continue
            parts = token.split()
            if len(parts) == 1:
                if parts[0] in words:
                    lat = (row.get("lat") or "").strip()
                    lon = (row.get("lon") or "").strip()
                    if lat and lon:
                        return name, lat, lon
            else:
                if all(p in words for p in parts):
                    lat = (row.get("lat") or "").strip()
                    lon = (row.get("lon") or "").strip()
                    if lat and lon:
                        return name, lat, lon
            # también podemos hacer búsqueda de substring
            if token in norm_text:
                lat = (row.get("lat") or "").strip()
                lon = (row.get("lon") or "").strip()
                if lat and lon:
                    return name, lat, lon
    return None