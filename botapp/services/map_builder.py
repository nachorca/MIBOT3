# -*- coding: utf-8 -*-
from __future__ import annotations
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
import os

import folium
from folium.plugins import MarkerCluster

from botapp.services.incidentes_db import init_db, migrate_db, get_incidentes
from botapp.services.incidentes_resolver import resolve_missing_coords

# Mapeo de colores por SICU
SICU_COLORS = {
    "Conflicto Armado": "red",
    "Terrorismo": "darkred",
    "Delincuencia": "orange",      # (Criminalidad)
    "Criminalidad": "orange",
    "Disturbios Civiles": "blue",
    "Hazards": "green",
}

# Normalizador de categoría a SICU canónica
def normalize_sicu(cat: str) -> str:
    if not cat:
        return "Otros"
    c = cat.strip().lower()
    if "conflicto" in c or "armed" in c:
        return "Conflicto Armado"
    if "terror" in c:
        return "Terrorismo"
    if "disturb" in c or "unrest" in c or "protest" in c or "riot" in c:
        return "Disturbios Civiles"
    if "hazard" in c or "natural" in c or "clima" in c or "meteo" in c:
        return "Hazards"
    if "crimen" in c or "delinc" in c or "crime" in c or "rob" in c or "asalt" in c or "secuest" in c:
        return "Criminalidad"
    return "Otros"

def _in_date_range(fecha_iso: str, days: int) -> bool:
    try:
        dt = datetime.fromisoformat(fecha_iso.replace("Z",""))
    except Exception:
        # formato alternativo
        try:
            dt = datetime.strptime(fecha_iso, "%Y-%m-%d %H:%M:%S")
        except Exception:
            return True  # si no sabemos, mostramos
    return dt >= datetime.utcnow() - timedelta(days=days)

def build_incidents_map(
    pais: Optional[str] = None,
    days: int = 7,
    center_lat: Optional[float] = None,
    center_lon: Optional[float] = None,
    zoom_start: int = 6,
    outfile_dir: str = "output/maps",
    outfile_name: Optional[str] = None,
    resolve_missing: bool = True,
) -> str:
    """
    Crea un mapa Folium con incidentes de incidentes.db.
    - pais: filtra por país (None = todos)
    - days: rango de días hacia atrás
    - center_lat/lon: centro del mapa (si None, intenta calcular un centro simple)
    - resolve_missing: ejecuta geocodificación de pendientes antes de pintar
    Devuelve la ruta del HTML generado.
    """
    init_db(); migrate_db()
    if resolve_missing:
        # hint de país para mejorar acierto cuando pintamos por país
        hint = pais if pais else None
        try:
            resolve_missing_coords(default_country_hint=hint)
        except Exception:
            pass

    rows = get_incidentes(pais=pais)

    # Filtramos por rango temporal y sólo con coordenadas válidas
    pts: List[Dict[str, Any]] = []
    for r in rows:
        if days and not _in_date_range(r.get("fecha") or "", days):
            continue
        lat, lon = r.get("lat"), r.get("lon")
        if lat is None or lon is None:
            continue
        pts.append(r)

    # Centro del mapa
    if center_lat is None or center_lon is None:
        if pts:
            # centro aproximado: promedio (simple y suficiente para arrancar)
            lat_avg = sum(p["lat"] for p in pts) / len(pts)
            lon_avg = sum(p["lon"] for p in pts) / len(pts)
            center_lat, center_lon = lat_avg, lon_avg
        else:
            # fallback global
            center_lat, center_lon = 25.0, 0.0

    # Construcción del mapa
    m = folium.Map(location=[center_lat, center_lon], zoom_start=zoom_start, tiles="OpenStreetMap")
    folium.TileLayer("cartodbpositron").add_to(m)
    folium.TileLayer("cartodbdark_matter").add_to(m)

    # Clusters por categoría SICU
    clusters_by_cat: Dict[str, MarkerCluster] = {}
    for cat in ("Conflicto Armado", "Terrorismo", "Criminalidad", "Disturbios Civiles", "Hazards", "Otros"):
        clusters_by_cat[cat] = MarkerCluster(name=cat, control=True, show=True)
        clusters_by_cat[cat].add_to(m)

    # Puntos
    for r in pts:
        cat_norm = normalize_sicu(r.get("categoria", ""))
        color = SICU_COLORS.get(cat_norm, "gray")
        fecha_txt = r.get("fecha", "")
        desc = r.get("descripcion", "").strip()
        place = r.get("place") or ""
        admin1 = r.get("admin1") or ""
        admin2 = r.get("admin2") or ""
        fuente = r.get("fuente") or ""
        extra = []
        if place: extra.append(f"<b>Lugar:</b> {place}")
        if admin1 or admin2: extra.append(f"<b>Admin:</b> {admin2}, {admin1}".strip(", "))
        if fuente: extra.append(f"<b>Fuente:</b> {fuente}")
        extra_html = "<br>".join(extra)

        popup = folium.Popup(
            f"<b>{cat_norm}</b><br>{desc}<br><i>{fecha_txt}</i><br>{extra_html}",
            max_width=450
        )

        folium.CircleMarker(
            location=[r["lat"], r["lon"]],
            radius=7,
            color=color,
            fill=True,
            fill_opacity=0.8,
            popup=popup,
        ).add_to(clusters_by_cat.get(cat_norm, clusters_by_cat["Otros"]))

    folium.LayerControl(collapsed=False).add_to(m)

    # Salida
    os.makedirs(outfile_dir, exist_ok=True)
    if not outfile_name:
        stamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        base = f"mapa_incidentes_{pais or 'todos'}_{days}d_{stamp}.html"
        outfile_name = base.replace(" ", "_")
    outpath = os.path.join(outfile_dir, outfile_name)
    m.save(outpath)
    return outpath
