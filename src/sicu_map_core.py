# -*- coding: utf-8 -*-
"""
Core de mapeo SICU: genera un mapa Folium a partir de un CSV con eventos.
- Geocodifica ubicaciones (solo nombre de lugar) con Nominatim + caché SQLite.
- Colorea por categoría SICU.
- Crea capas por categoría y cluster global de marcadores.
- Devuelve las rutas del HTML generado y del CSV de no geocodificados (si existe).

Uso desde Python:
    from src.sicu_map_core import generate_sicu_map
    html_path, missing_path = generate_sicu_map(
        csv_path="data/eventos_sicu.csv",
        out_html="output/mapa_eventos_sicu.html",
        cfg_path="sicu_config.yaml",
        user_email="info@santiagolegalconsulting.es"
    )
"""
import os
import sqlite3
from html import escape
from typing import Optional, Tuple

import folium
import pandas as pd
import yaml
from folium.plugins import MarkerCluster
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter

import unicodedata
import re

def _norm(s: str) -> str:
    """Normaliza texto: minúsculas, sin acentos, sin espacios extra ni signos raros."""
    s = str(s).strip()
    s = unicodedata.normalize("NFKD", s)
    s = s.encode("ascii", "ignore").decode("ascii")
    s = s.lower()
    s = re.sub(r"\s+", " ", s)
    s = s.replace("_", " ")
    return s

def _find_col(df: pd.DataFrame, expected: str, fallbacks: list[str]) -> str | None:
    """Devuelve el nombre REAL de la columna en df que coincide con expected/fallbacks tras normalizar."""
    cols_norm = { _norm(c): c for c in df.columns }
    candidates = [_norm(expected)] + [_norm(x) for x in fallbacks]
    for cand in candidates:
        if cand in cols_norm:
            return cols_norm[cand]
    return None

def _resolve_columns(df: pd.DataFrame, cfg_cols: dict) -> dict:
    """
    Resuelve nombres de columnas de forma tolerante (acentos, mayúsculas, sinónimos).
    Devuelve un dict con las claves estándar -> nombre real en el DataFrame.
    """
    synonyms = {
        "location": ["localizacion", "localidad", "lugar", "ubicacion", "ubicación", "place", "location", "sitio"],
        "category": ["categoria sicu", "categoria", "category", "sicu", "tipo", "clasificacion", "clasificación"],
        "description": ["breve descripcion", "descripcion", "descripción", "detalle", "resumen", "observaciones", "descripcion breve"],
        "date": ["fecha", "date", "dia"],
        "time": ["hora", "time"],
        "severity": ["nivel de severidad", "severidad", "nivel", "gravidad", "severity"],
        "subcategory": ["subcategoria", "subcategoría", "subcat", "tipo detalle", "modalidad"],
    }
    resolved = {}
    for key in ("location", "category", "description", "date", "time", "severity", "subcategory"):
        expected = cfg_cols.get(key)
        fb = synonyms.get(key, [])
        real = _find_col(df, expected or "", fb)
        if real:
            resolved[key] = real
        else:
            # Solo obligatorias fallan duro; opcionales pueden faltar
            if key in ("location", "category", "description"):
                raise ValueError(
                    f"No se encontró la columna '{expected}' (clave '{key}'). "
                    f"Columnas disponibles: {list(df.columns)}"
                )
            else:
                resolved[key] = None
    return resolved


# ---------------------------
# Caché de geocodificación (SQLite)
# ---------------------------
class GeoCache:
    def __init__(self, path: str = "cache_geocoding.sqlite"):
        self.path = path
        self.conn = sqlite3.connect(self.path)
        self._ensure_table()

    def _ensure_table(self):
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS geocache (
                q   TEXT PRIMARY KEY,
                lat REAL,
                lng REAL,
                raw TEXT
            )
            """
        )
        self.conn.commit()

    def get(self, q: str):
        cur = self.conn.execute("SELECT lat, lng FROM geocache WHERE q = ?", (q,))
        row = cur.fetchone()
        return (row[0], row[1]) if row else None

    def set(self, q: str, lat: float, lng: float, raw: str = ""):
        self.conn.execute(
            "INSERT OR REPLACE INTO geocache (q, lat, lng, raw) VALUES (?, ?, ?, ?)",
            (q, lat, lng, raw),
        )
        self.conn.commit()

    def close(self):
        self.conn.close()


# ---------------------------
# Utilidades
# ---------------------------
def _load_cfg(cfg_path: str) -> dict:
    if not os.path.exists(cfg_path):
        raise FileNotFoundError(
            f"No se encontró el archivo de configuración: {cfg_path}. "
            f"Crea 'sicu_config.yaml' antes de generar el mapa."
        )
    with open(cfg_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def _make_geocode(user_email: Optional[str] = None):
    # Cumple la política de Nominatim: user_agent identificable + (opcional) email
    ua = "sicu-mapper/1.0"
    if user_email:
        ua += f" ({user_email})"
    geolocator = Nominatim(user_agent=ua, timeout=10)
    # Respetar límites (≈1 req/seg) y tragar excepciones de red para no romper el flujo
    return RateLimiter(geolocator.geocode, min_delay_seconds=1, swallow_exceptions=True)


def _smart_q(place: str, default_country: Optional[str]) -> str:
    if default_country and default_country.lower() not in place.lower():
        return f"{place}, {default_country}"
    return place


def _popup(row: pd.Series, cols: dict) -> str:
    parts = []
    parts.append(f"<b>{escape(str(row.get(cols['category'], '')))}</b>")
    desc = escape(str(row.get(cols["description"], "")))
    if desc:
        parts.append(desc)
    for opt in ("date", "time", "severity", "subcategory"):
        col = cols.get(opt)
        if col and col in row and str(row[col]).strip():
            parts.append(f"<small><b>{opt.capitalize()}:</b> {escape(str(row[col]))}</small>")
    return "<br>".join(parts)


def _base_map(cfg: dict) -> folium.Map:
    tiles_cfg = cfg.get("tiles", {}) or {}
    center = cfg.get("map", {}) or {}
    lat = center.get("center_lat", 0.0)
    lng = center.get("center_lng", 0.0)
    zoom = center.get("zoom_start", 2)
    provider = tiles_cfg.get("provider", "OpenStreetMap")
    return folium.Map(location=[lat, lng], zoom_start=zoom, tiles=provider)


# Helper para leer CSV de manera robusta
def _read_csv_safely(csv_path: str) -> pd.DataFrame:
    """
    Lee el CSV intentando detectar separador; si falla, hace fallback a ',' y luego a ';'.
    Tolera filas mal formadas (on_bad_lines='skip') y BOM (utf-8-sig).
    Lanza un ValueError si el archivo está vacío o no se puede parsear.
    """
    if os.path.getsize(csv_path) == 0:
        raise ValueError(f"El CSV está vacío: {csv_path}")
    common_kwargs = dict(engine="python", encoding="utf-8-sig", on_bad_lines="skip")
    # intento 1: autodetección
    try:
        return pd.read_csv(csv_path, sep=None, **common_kwargs)
    except Exception:
        pass
    # intento 2: coma
    try:
        return pd.read_csv(csv_path, sep=",", quotechar='"', escapechar="\\", **common_kwargs)
    except Exception:
        pass
    # intento 3: punto y coma
    try:
        return pd.read_csv(csv_path, sep=";", quotechar='"', escapechar="\\", **common_kwargs)
    except Exception as e:
        raise ValueError(f"No se pudo leer el CSV con autodetección, ',' ni ';'. Detalle: {e}")


# ---------------------------
# API principal
# ---------------------------
def generate_sicu_map(
    csv_path: str,
    out_html: str = "mapa_eventos_sicu.html",
    cfg_path: str = "sicu_config.yaml",
    user_email: Optional[str] = None,
) -> Tuple[str, Optional[str]]:
    """
    Genera el mapa HTML y, si hay ubicaciones sin coordenadas, exporta un CSV de no geocodificados.
    Devuelve (ruta_html, ruta_no_geocodificados | None)
    """
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"No existe el CSV: {csv_path}")

    cfg = _load_cfg(cfg_path)
    cols = cfg["columns"]
    color_map = cfg["category_colors"]
    default_country = cfg.get("default_country")

    # Cargar CSV con separador automático robusto
    df = _read_csv_safely(csv_path)

    # Resolver nombres reales de columnas (tolerante a acentos y sinónimos)
    res_cols = _resolve_columns(df, cols)

    # Validación de columnas mínimas
    for required in ("location", "category", "description"):
        colname = res_cols.get(required)
        if not colname or colname not in df.columns:
            raise ValueError(f"Falta columna requerida en CSV: {colname} (clave '{required}')")

    # Geocodificación
    cache = GeoCache(cfg.get("cache_file", "cache_geocoding.sqlite"))
    geocode = _make_geocode(user_email)
    lat_col, lng_col = "_lat", "_lng"
    df[lat_col], df[lng_col] = None, None

    for i, row in df.iterrows():
        place = str(row[res_cols["location"]]).strip()
        if not place:
            continue
        q = _smart_q(place, default_country)
        cached = cache.get(q)
        if cached:
            lat, lng = cached
        else:
            loc = geocode(q)
            lat, lng = (
                (loc.latitude, loc.longitude)
                if (loc and getattr(loc, "latitude", None) and getattr(loc, "longitude", None))
                else (None, None)
            )
            if lat is not None:
                cache.set(q, lat, lng, "")

        df.at[i, lat_col], df.at[i, lng_col] = lat, lng

    # Export de fallos de geocodificación (si los hay)
    missing_path = None
    missing = df[df[lat_col].isna() | df[lng_col].isna()]
    if len(missing) > 0:
        out_dir = os.path.dirname(out_html) or "."
        os.makedirs(out_dir, exist_ok=True)
        missing_path = os.path.join(out_dir, "no_geocodificados.csv")
        missing.to_csv(missing_path, index=False, encoding="utf-8")

    # Construcción del mapa
    m = _base_map(cfg)
    layer_groups = {cat: folium.FeatureGroup(name=cat, show=True).add_to(m) for cat in color_map}
    cluster = MarkerCluster(name="Eventos (cluster)").add_to(m)

    for _, r in df.dropna(subset=[lat_col, lng_col]).iterrows():
        cat = str(r.get(res_cols["category"], "")).strip()
        color = color_map.get(cat, "gray")
        tooltip = str(r.get(res_cols["location"], "")).strip() or None

        marker = folium.Marker(
            location=[r[lat_col], r[lng_col]],
            popup=folium.Popup(_popup(r, res_cols), max_width=350),
            tooltip=tooltip,
            icon=folium.Icon(color=color, icon="info-sign"),
        )
        if cat in layer_groups:
            marker.add_to(layer_groups[cat])
        marker.add_to(cluster)

    folium.LayerControl(collapsed=False).add_to(m)

    # Guardar
    os.makedirs(os.path.dirname(out_html) or ".", exist_ok=True)
    m.save(out_html)
    cache.close()
    return out_html, missing_path
