# botapp/utils/incidentes_csv.py
# -*- coding: utf-8 -*-
"""
TXT diario (data/<pais>/<YYYY-MM-DD>.txt) → CSV de incidentes por país/día.

Flujo:
- Leer el archivo TXT del día.
- Extraer bloques de noticias.
- Traducir al español (si aplica).
- Limpiar y resumir el texto.
- Clasificar según SICU (Conflicto Armado, Terrorismo, etc.).
- Localizar usando solo el gazetteer del país (sin LLM).
- Generar/actualizar un CSV en output/incidentes/<pais>/incidentes_<pais>_<YYYY-MM-DD>.csv.
"""

from __future__ import annotations
from pathlib import Path
from typing import List, Dict, Tuple, Optional
import csv
import re
import unicodedata
from datetime import datetime

from botapp.config import get_settings
from botapp.utils.translator import to_spanish_excerpt  # traductor HF/Argos
# from botapp.services.llm_client import get_client  # Eliminado: uso solo gazetteer

from botapp.utils.gazetteer import load_gazetteer, match_location

SET = get_settings()

DATA_DIR = Path(SET.data_dir).resolve()  # e.g., ./data
OUTPUT_DIR = Path(SET.data_dir).resolve().parent / "output"
INCIDENTS_DIR = OUTPUT_DIR / "incidentes"
GAZETTEER_DIR = DATA_DIR / "gazetteer"

CSV_FIELDS = [
    "fecha",
    "hora",
    "pais",
    "categoria_sicu",
    "descripcion",
    "localizacion",
    "lat",
    "lon",
    "fuente",
]

HEADER_RE = re.compile(
    r"^---\s+(?P<channel>.+?)\s+@\s+(?P<dt>\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2})\s+---\s*$"
)
URL_RE = re.compile(r"https?://\S+")
HASHTAG_RE = re.compile(r"#\S+")
URL_EXTRACT_RE = re.compile(r"(https?://\S+)", re.IGNORECASE)

def _slugify_country(raw: str) -> str:
    if not raw:
        return ""
    s = raw.strip().lower()
    s_norm = "".join(
        c for c in unicodedata.normalize("NFD", s)
        if unicodedata.category(c) != "Mn"
    )
    aliases = {
        "haiti": "haiti",
        "libia": "libia",
        "libya": "libia",
        "gaza": "gaza",
        "colombia": "colombia",
        "campello": "campello",
        "el campello": "campello",
        "mali": "mali",
    }
    return aliases.get(s_norm, s_norm)

def _normalize_sicu(text_es: str) -> str:
    t = (text_es or "").lower()
    if any(k in t for k in [
        "combate", "enfrentamiento", "tiroteo", "disparos",
        "militar", "fuerzas armadas"
    ]):
        return "Conflicto Armado"
    if any(k in t for k in [
        "bomba", "explosión", "atentado", "terrorista"
    ]):
        return "Terrorismo"
    if any(k in t for k in [
        "atraco", "robo", "asesinato", "pandilla", "drogas"
    ]):
        return "Criminalidad"
    if any(k in t for k in [
        "protesta", "manifestación", "disturbios", "huelga"
    ]):
        return "Disturbios Civiles"
    if any(k in t for k in [
        "inundación", "terremoto", "incendio", "deslizamiento",
        "tormenta", "ciclón"
    ]):
        return "Hazards"
    return "Otros"

def _ensure_headers(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        with path.open("w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=CSV_FIELDS)
            w.writeheader()

def _read_existing(path: Path) -> List[Dict]:
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))

def _dedup_rows(rows: List[Dict]) -> List[Dict]:
    seen = set()
    out: List[Dict] = []
    for r in rows:
        sig = (
            (r.get("fecha") or "").strip(),
            (r.get("hora") or "").strip(),
            (r.get("pais") or "").strip().lower(),
            (r.get("descripcion") or "").strip().lower(),
            (r.get("fuente") or "").strip().lower(),
        )
        if sig in seen:
            continue
        seen.add(sig)
        out.append(r)
    return out

def _parse_txt_news(txt_path: Path) -> List[Dict[str, str]]:
    try:
        text = txt_path.read_text(encoding="utf-8", errors="ignore")
    except Exception:
        return []
    lines = text.splitlines()
    entries: List[Dict[str, str]] = []
    current: Optional[Dict[str, str]] = None
    body_lines: List[str] = []
    for line in lines:
        m = HEADER_RE.match(line)
        if m:
            if current is not None:
                current["body"] = "\n".join(body_lines).strip()
                entries.append(current)
            current = {
                "channel": m.group("channel").strip(),
                "dt": m.group("dt").strip(),
            }
            body_lines = []
        else:
            if current is not None:
                body_lines.append(line)
    if current is not None:
        current["body"] = "\n".join(body_lines).strip()
        entries.append(current)
    return entries

def _clean_summary(text: str) -> str:
    if not text:
        return ""
    text = URL_RE.sub("", text)
    text = HASHTAG_RE.sub("", text)
    lines = [l.strip() for l in text.splitlines()]
    cleaned_lines = [l for l in lines if l and not l.lower().startswith("via ")]
    text = " ".join(cleaned_lines)
    sent_split = re.split(r"(?<=[\.\?\!])\s+", text)
    if len(sent_split) > 2:
        text = " ".join(sent_split[:2])
    if len(text) > 600:
        text = text[:597].rstrip() + "..."
    return text.strip()

def _extract_urls(text: str) -> List[str]:
    if not text:
        return []
    return URL_EXTRACT_RE.findall(text)

def _load_gazetteer(slug: str) -> List[Dict[str, str]]:
    gfile = GAZETTEER_DIR / f"{slug}.csv"
    if not gfile.exists():
        return []
    out: List[Dict[str, str]] = []
    try:
        with gfile.open(newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                out.append(row)
    except Exception as e:
        print(f"[incidentes_csv] Error leyendo gazetteer {gfile}: {e!r}")
    return out

def _norm(s: str) -> str:
    if not s:
        return ""
    s = unicodedata.normalize("NFD", s)
    s = "".join(c for c in s if unicodedata.category(c) != "Mn")
    return s.lower()

def _match_location_with_gazetteer(
    text: str,
    gaz: List[Dict[str, str]],
) -> Optional[Tuple[str, str, str]]:
    if not text or not gaz:
        return None
    norm_text = _norm(text)
    words = set(re.findall(r"\w+", norm_text))
    for row in gaz:
        name = (row.get("name") or "").strip()
        aliases = (row.get("aliases") or "").split("|") if row.get("aliases") else []
        candidates = [name] + [a.strip() for a in aliases if a.strip()]
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
            if token in norm_text:
                lat = (row.get("lat") or "").strip()
                lon = (row.get("lon") or "").strip()
                if lat and lon:
                    return name, lat, lon
    return None

def save_incidentes_csv_from_txt(
    country: str,
    day_iso: str,
) -> Tuple[Path, int]:
    slug = _slugify_country(country)
    txt_path = DATA_DIR / slug / f"{day_iso}.txt"
    country_name = slug.capitalize() or country.capitalize()

    country_dir = INCIDENTS_DIR / slug
    country_dir.mkdir(parents=True, exist_ok=True)
    out_csv = country_dir / f"incidentes_{slug}_{day_iso}.csv"

    _ensure_headers(out_csv)
    existing = _read_existing(out_csv)

    if not txt_path.exists():
        return out_csv, len(existing)

    entries = _parse_txt_news(txt_path)
    if not entries:
        return out_csv, len(existing)

    gaz = _load_gazetteer(slug)

    new_rows: List[Dict[str, str]] = []

    for e in entries:
        dt_raw = e.get("dt") or ""
        fecha = day_iso
        hora = ""
        try:
            dt_obj = datetime.fromisoformat(dt_raw)
            fecha = dt_obj.date().isoformat()
            hora = dt_obj.time().isoformat(timespec="seconds")
        except Exception:
            fecha = day_iso

        body_orig = (e.get("body") or "").strip()
        fuente = (e.get("channel") or "").strip()

        urls = _extract_urls(body_orig)

        try:
            raw_es = to_spanish_excerpt(body_orig, max_chars=1000) or body_orig
        except Exception:
            raw_es = body_orig

        body_resumen = _clean_summary(raw_es) or raw_es
        categoria_sicu = _normalize_sicu(body_resumen)

        if urls:
            urls_block = " ".join(f"[Enlace: {u}]" for u in urls)
            descripcion_final = f"{body_resumen}  {urls_block}"
        else:
            descripcion_final = body_resumen

        loc = ""
        lat = ""
        lon = ""

        if gaz:
            m = _match_location_with_gazetteer(body_resumen, gaz)
            if not m:
                m = _match_location_with_gazetteer(body_orig, gaz)
            if m:
                loc, lat, lon = m

        row = {
            "fecha": fecha,
            "hora": hora,
            "pais": country_name,
            "categoria_sicu": categoria_sicu,
            "descripcion": descripcion_final,
            "localizacion": loc,
            "lat": lat,
            "lon": lon,
            "fuente": fuente,
        }
        new_rows.append(row)

    combined = _dedup_rows(existing + new_rows)

    with out_csv.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        w.writeheader()
        w.writerows(combined)

    return out_csv, len(combined)