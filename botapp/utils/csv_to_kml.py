# -*- coding: utf-8 -*-
# botapp/utils/csv_to_kml.py
from __future__ import annotations

from pathlib import Path
from typing import Dict, List, Tuple
import csv
import html

# ============================================================
#   CONFIG BÁSICA
# ============================================================

# Raíz del proyecto MIBOT3 (botapp/utils → ../..)
BASE_DIR = Path(__file__).resolve().parents[2]
OUTPUT_DIR = BASE_DIR / "output"
GAZETTEER_DIR = BASE_DIR / "data" / "gazetteer"

# KML usa colores aabbggrr (alpha, blue, green, red)
SICU_STYLES: Dict[str, str] = {
    "Conflicto Armado":   "ff0000ff",   # rojo
    "Terrorismo":         "ff000000",   # negro
    "Criminalidad":       "ff00a5ff",   # azul
    "Disturbios Civiles": "ffff0000",   # cian
    "Hazards":            "ff008000",   # verde
    "Otros":              "ff808080",   # gris
}

# columnas mínimas esperadas en el CSV SICU
REQUIRED_COLUMNS = [
    "categoria_sicu",
    "descripcion",
    "fecha",
    "hora",
    "localizacion",
    "lat",
    "lon",
]

# aliases para normalizar cabeceras del CSV SICU
HEADER_ALIASES = {
    "categoría sicu":      "categoria_sicu",
    "categoria sicu":      "categoria_sicu",
    "categoría_sicu":      "categoria_sicu",
    "categoria":           "categoria_sicu",
    "categoría":           "categoria_sicu",
    "breve descripción":   "descripcion",
    "breve descripcion":   "descripcion",
    "descripcion":         "descripcion",
    "descripción":         "descripcion",
    "fecha":               "fecha",
    "hora":                "hora",
    "localización":        "localizacion",
    "localizacion":        "localizacion",
    "lat":                 "lat",
    "lon":                 "lon",
}

# ============================================================
#   HELPERS BÁSICOS
# ============================================================

def _to_float(val: str | None) -> float | None:
    if val is None:
        return None
    s = str(val).strip()
    if not s:
        return None
    # permitir coma decimal
    if "," in s and "." not in s:
        s = s.replace(",", ".")
    try:
        return float(s)
    except Exception:
        return None


def _style_for(cat: str) -> str:
    c = (cat or "").strip()
    if c in SICU_STYLES:
        return c.replace(" ", "_").lower()
    return "otros"


def _kml_header(name: str = "Incidentes SICU") -> str:
    styles = []
    for cat, color in SICU_STYLES.items():
        sid = cat.replace(" ", "_").lower()
        styles.append(f"""
    <Style id="{sid}">
      <IconStyle>
        <color>{color}</color>
        <scale>1.1</scale>
        <Icon>
          <href>http://maps.google.com/mapfiles/kml/paddle/wht-blank.png</href>
        </Icon>
      </IconStyle>
      <LabelStyle>
        <color>ff000000</color>
        <scale>0.9</scale>
      </LabelStyle>
    </Style>""")
    return f"""<?xml version="1.0" encoding="UTF-8"?>
<kml xmlns="http://www.opengis.net/kml/2.2">
  <Document>
    <name>{html.escape(name)}</name>
    <open>1</open>
    {''.join(styles)}
"""


def _kml_footer() -> str:
    return "  </Document>\n</kml>\n"


def _placemark(name: str, desc: str, lat: float, lon: float, style_id: str) -> str:
    return f"""
    <Placemark>
      <name>{html.escape(name)}</name>
      <styleUrl>#{style_id}</styleUrl>
      <visibility>1</visibility>
      <description><![CDATA[{desc}]]></description>
      <Point>
        <coordinates>{lon:.6f},{lat:.6f},0</coordinates>
      </Point>
    </Placemark>"""


# ============================================================
#   GAZETTEER (LOCALIZACIONES) – ROBUSTO Y JERÁRQUICO
# ============================================================

def _load_gazetteer(country: str) -> Tuple[List[Dict[str, str]], Dict[str, str]]:
    """
    Carga el gazetteer para un país desde:
      data/gazetteer/<country>.csv

    Soporta cabeceras tipo:
      name / Name / NOMBRE
      admin1 / Admin1 / ADM1
      admin2 / Admin2 / ADM2
      lat / Lat / LAT / latitude
      lon / Lon / LNG / longitude
      aliases / Aliases
      kind / tipo / type

    Devuelve:
      - lista de filas (dict original)
      - mapping colmap { 'name': colname_real, 'lat': colname_real, ... }
    """
    country = (country or "").strip().lower()
    gfile = GAZETTEER_DIR / f"{country}.csv"
    if not gfile.exists():
        print(f"[csv_to_kml] No hay gazetteer para {country}: {gfile}")
        return [], {}

    rows: List[Dict[str, str]] = []
    colmap: Dict[str, str] = {}

    try:
        with gfile.open(newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            # Normalizar cabeceras
            for h in reader.fieldnames or []:
                key = (h or "").strip()
                low = key.lower()
                if low in {"name", "nombre", "localidad", "city", "town"}:
                    colmap["name"] = key
                elif low in {"admin1", "adm1", "provincia", "departamento"}:
                    colmap["admin1"] = key
                elif low in {"admin2", "adm2", "municipio", "distrito", "commune"}:
                    colmap["admin2"] = key
                elif low in {"lat", "latitude", "y"}:
                    colmap["lat"] = key
                elif low in {"lon", "long", "lng", "longitude", "x"}:
                    colmap["lon"] = key
                elif low in {"aliases", "alias"}:
                    colmap["aliases"] = key
                elif low in {"kind", "tipo", "type"}:
                    colmap["kind"] = key
            # Leer filas
            for row in reader:
                rows.append(row)
    except Exception as e:
        print(f"[csv_to_kml] Error leyendo gazetteer {gfile}: {e!r}")
        return [], {}

    return rows, colmap


def _row_get(row: Dict[str, str], colmap: Dict[str, str], key: str) -> str:
    real = colmap.get(key)
    if not real:
        return ""
    return (row.get(real) or "").strip()


def _kind_score(kind: str) -> int:
    """
    Asigna prioridad según tipo de localización:
      airport > official > neighbourhood > town > city > other
    """
    k = (kind or "").strip().lower()
    if k in {"airport", "aeropuerto", "aéroport"}:
        return 100
    if k in {"official", "palace", "embassy", "gov", "government"}:
        return 90
    if k in {"neighbourhood", "barrio", "district"}:
        return 80
    if k in {"town", "village", "pueblo"}:
        return 70
    if k in {"city", "ciudad"}:
        return 60
    return 50  # genérico


def _lookup_coords_in_gazetteer_from_loc(loc: str, gzt: List[Dict[str, str]], colmap: Dict[str, str]) -> Tuple[float | None, float | None]:
    """
    Intenta extraer coordenadas del gazetteer a partir del campo 'Localización'.

    Estrategia:
      - Tomar segmentos separados por comas:
            "Croix-des-Bouquets, Port-au-Prince, Ouest, Haiti"
      - Probar cada segmento (más específico primero) contra name/aliases.
      - Si hay varios candidatos, elegir el de mayor kind_score.
    """
    if not loc or not gzt:
        return (None, None)

    segments = [s.strip() for s in loc.split(",") if s.strip()]
    if not segments:
        return (None, None)

    best_lat = best_lon = None
    best_score = -1

    for seg in segments:  # de más específico a más general
        search = seg.lower()
        for row in gzt:
            name = _row_get(row, colmap, "name")
            aliases = _row_get(row, colmap, "aliases").split("|")
            kind = _row_get(row, colmap, "kind") or "city"

            candidates = [name] + aliases
            for c in candidates:
                c_clean = c.strip()
                if not c_clean:
                    continue
                if c_clean.lower() == search:
                    try:
                        lat = float(_row_get(row, colmap, "lat"))
                        lon = float(_row_get(row, colmap, "lon"))
                    except Exception:
                        continue
                    score = _kind_score(kind)
                    if score > best_score:
                        best_score = score
                        best_lat, best_lon = lat, lon

    return (best_lat, best_lon)


def _lookup_coords_in_gazetteer_from_desc(desc: str, gzt: List[Dict[str, str]], colmap: Dict[str, str]) -> Tuple[float | None, float | None]:
    """
    Intenta inferir la localización A PARTIR DE LA DESCRIPCIÓN, usando el gazetteer.

    Estrategia:
      - Convertir descripción a lower.
      - Si 'name' o 'aliases' del gazetteer aparecen como substring → candidato.
      - Priorizar por kind_score (airport, official, barrio, etc.).
      - Se devuelve el mejor candidato.
    """
    if not desc or not gzt:
        return (None, None)

    text = desc.lower()

    best_lat = best_lon = None
    best_score = -1

    for row in gzt:
        name = _row_get(row, colmap, "name")
        aliases = _row_get(row, colmap, "aliases").split("|")
        kind = _row_get(row, colmap, "kind") or "city"

        candidates = [name] + aliases
        for c in candidates:
            c_clean = c.strip()
            if not c_clean:
                continue
            token = c_clean.lower()
            if not token:
                continue
            if token in text:
                try:
                    lat = float(_row_get(row, colmap, "lat"))
                    lon = float(_row_get(row, colmap, "lon"))
                except Exception:
                    continue
                score = _kind_score(kind)
                if score > best_score:
                    best_score = score
                    best_lat, best_lon = lat, lon

    return (best_lat, best_lon)

# ============================================================
#   HEURÍSTICAS PARA LIBIA (SI GAZETTEER FALLA)
# ============================================================

def _heuristic_coords_libya(
    categoria: str,
    loc: str,
    desc: str,
    gzt: List[Dict[str, str]],
    colmap: Dict[str, str],
) -> Tuple[float | None, float | None, str | None]:
    """
    Heurística para LIBIA:
      - Si el texto menciona Benghazi, Sirte, Misrata, Sabha, Derna, Tobruk → usar esa ciudad.
      - Si no se menciona nada reconocible → usar Tripoli por defecto.
    Devuelve (lat, lon, nombre_ciudad) o (None,None,None).
    """
    if not gzt or not colmap:
        return None, None, None

    texto = f"{loc} {desc}".lower()

    heuristic_targets = [
        ("Benghazi", ["benghazi", "بنغازي", "banġāzī"]),
        ("Sirte", ["sirte", "سرت", "surt"]),
        ("Misrata", ["misrata", "مصراتة", "miṣrāta"]),
        ("Sabha", ["sabha", "sebha", "سبها"]),
        ("Derna", ["derna", "darna", "درنة", "darnah"]),
        ("Tobruk", ["tobruk", "ṭubruq", "طبرق"]),
    ]

    target_name = None
    for name, keywords in heuristic_targets:
        if any(k in texto for k in keywords):
            target_name = name
            break

    if target_name is None:
        target_name = "Tripoli"

    target_lower = target_name.lower()
    for row in gzt:
        nm = _row_get(row, colmap, "name").lower()
        if nm == target_lower:
            try:
                lat = float(_row_get(row, colmap, "lat"))
                lon = float(_row_get(row, colmap, "lon"))
                return lat, lon, target_name
            except Exception:
                continue

    return None, None, None

# ============================================================
#   CSV → KML
# ============================================================

def csv_to_kml(
    csv_path: str | Path,
    out_path: str | Path | None = None,
    day_iso: str | None = None,
    enrich: bool = False,
    country: str | None = None,
) -> str:
    """
    Convierte un CSV con columnas SICU a KML.

    Espera columnas (o alias) en el CSV SICU:

      Categoría SICU / categoria_sicu / categoria
      Breve descripción / descripcion
      Fecha
      Hora
      Localización / localizacion
      Lat / lat
      Lon / lon

    - Si enrich=True y hay gazetteer para el país:
        * Si lat/lon están vacíos:
            1) Intentar Localización -> gazetteer (segmentos por coma).
            2) Si falla, intentar Descripción -> gazetteer (substring).
            3) Si país es LIBIA y sigue fallando → heurística de ciudad (Tripoli o Benghazi/Sirte/Misrata/Sabha/Derna/Tobruk).
    - Ignora filas sin lat/lon válidos al final.
    - Crea KML agrupado por categoría SICU.

    Devuelve la ruta (str) del KML generado.
    """
    csv_p = Path(csv_path)
    if out_path is not None:
        out_p = Path(out_path)
    else:
        out_p = csv_p.with_suffix(".kml")

    # Leer CSV SICU
    with csv_p.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows_raw = list(reader)

    if not rows_raw:
        out_p.parent.mkdir(parents=True, exist_ok=True)
        out_p.write_text(_kml_header() + _kml_footer(), encoding="utf-8")
        return str(out_p)

    # Normalizar cabeceras -> mapa origen→destino
    fieldmap: Dict[str, str] = {}
    for h in reader.fieldnames or []:
        key = (h or "").strip()
        low = key.lower()
        dest = HEADER_ALIASES.get(low, low)
        fieldmap[key] = dest

    # Cargar gazetteer si se pide enriquecimiento
    gzt: List[Dict[str, str]] = []
    colmap_gzt: Dict[str, str] = {}
    if enrich and country:
        gzt, colmap_gzt = _load_gazetteer(country)

    country_norm = (country or "").strip().lower()

    # Construir lista de filas normalizadas
    norm_rows: List[Dict[str, str]] = []
    for r in rows_raw:
        nr: Dict[str, str] = {k: "" for k in REQUIRED_COLUMNS}
        for src_key, val in r.items():
            if src_key not in fieldmap:
                continue
            dest = fieldmap[src_key]
            if dest in nr:
                nr[dest] = (val or "").strip()

        # Enriquecer lat/lon si están vacías
        if enrich and gzt and colmap_gzt:
            lat_v = nr.get("lat", "")
            lon_v = nr.get("lon", "")
            latf = _to_float(lat_v)
            lonf = _to_float(lon_v)

            if latf is None or lonf is None:
                # 1) Intentar con Localización
                lat_g, lon_g = _lookup_coords_in_gazetteer_from_loc(
                    nr.get("localizacion", ""), gzt, colmap_gzt
                )

                # 2) Si sigue fallando, intentar con la Descripción
                if lat_g is None or lon_g is None:
                    lat_g, lon_g = _lookup_coords_in_gazetteer_from_desc(
                        nr.get("descripcion", ""), gzt, colmap_gzt
                    )

                # 3) Si sigue fallando y país = Libia → HEURÍSTICA
                if (lat_g is None or lon_g is None) and country_norm in {"libia", "libya"}:
                    cat = nr.get("categoria_sicu", "")
                    loc = nr.get("localizacion", "")
                    desc = nr.get("descripcion", "")
                    lat_h, lon_h, city = _heuristic_coords_libya(cat, loc, desc, gzt, colmap_gzt)
                    if lat_h is not None and lon_h is not None:
                        lat_g, lon_g = lat_h, lon_h
                        if not loc:
                            nr["localizacion"] = f"{city} (estimado)"

                if lat_g is not None and lon_g is not None:
                    nr["lat"] = f"{lat_g:.6f}"
                    nr["lon"] = f"{lon_g:.6f}"

        norm_rows.append(nr)

    # Construir placemarks
    placemarks_by_style: Dict[str, List[str]] = {}
    total = 0
    sin_coord = 0

    for r in norm_rows:
        cat = r.get("categoria_sicu", "") or ""
        desc = r.get("descripcion", "") or ""
        fecha = r.get("fecha", "") or ""
        hora = r.get("hora", "") or ""
        loc = r.get("localizacion", "") or ""
        lat = _to_float(r.get("lat"))
        lon = _to_float(r.get("lon"))

        total += 1
        if lat is None or lon is None:
            sin_coord += 1
            continue

        style_id = _style_for(cat)
        name = f"{cat or 'Incidente'} — {fecha} {hora}".strip()
        popup = (
            f"<b>Categoría:</b> {html.escape(cat or 'N/D')}<br>"
            f"<b>Fecha:</b> {html.escape(fecha)} {html.escape(hora)}<br>"
            f"<b>Localización:</b> {html.escape(loc or 'N/D')}<br>"
            f"<b>Descripción:</b> {html.escape(desc or '')}"
        )

        pm = _placemark(name, popup, lat, lon, style_id)
        placemarks_by_style.setdefault(style_id, []).append(pm)

    print(f"[csv_to_kml] total filas CSV: {total}, sin coordenadas tras gazetteer/heurística: {sin_coord}")

    # Si no hay ningún punto válido, generar KML mínimo (sin placemarks)
    if not placemarks_by_style:
        out_p.parent.mkdir(parents=True, exist_ok=True)
        out_p.write_text(_kml_header(out_p.stem) + _kml_footer(), encoding="utf-8")
        return str(out_p)

    parts: List[str] = [_kml_header(out_p.stem)]
    for style_id, pms in placemarks_by_style.items():
        folder_name = style_id.replace("_", " ").title()
        parts.append(f"    <Folder>\n      <name>{html.escape(folder_name)}</name>\n")
        parts.extend(pms)
        parts.append("\n    </Folder>\n")

    parts.append(_kml_footer())

    out_p.parent.mkdir(parents=True, exist_ok=True)
    out_p.write_text("".join(parts), encoding="utf-8")

    return str(out_p)