from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Iterable, Optional, Sequence
import html

import folium
from folium import FeatureGroup
from branca.element import Element

from botapp.config import get_settings
from botapp.services.incidentes_db import (
    init_db,
    migrate_db,
    get_incidentes_geocodificados,
)
from botapp.services.incidentes_styles import (
    SICUCatalog,
    load_sicu_catalog,
    CategoryStyle,
)


def _format_dt(value: Optional[str]) -> str:
    if not value:
        return ""
    try:
        dt = datetime.fromisoformat(value)
        return dt.strftime("%Y-%m-%d %H:%M")
    except Exception:
        return value


def _is_url(text: Optional[str]) -> bool:
    if not text:
        return False
    t = text.strip().lower()
    return t.startswith("http://") or t.startswith("https://")


def _build_popup_html(
    incidente: dict,
    style: CategoryStyle,
) -> str:
    descripcion = incidente.get("descripcion") or "Sin descripción disponible."
    fuente = incidente.get("fuente") or ""
    place = incidente.get("place") or ""
    pais = incidente.get("pais") or ""
    accuracy = incidente.get("accuracy") or ""
    created = _format_dt(incidente.get("created_at"))
    updated = _format_dt(incidente.get("updated_at"))

    fuente_html = ""
    if fuente:
        if _is_url(fuente):
            safe_url = html.escape(fuente, quote=True)
            fuente_html = f'<a href="{safe_url}" target="_blank" rel="noopener">Fuente</a>'
        else:
            fuente_html = html.escape(fuente)

    parts = [
        f"<h4 style='margin:0 0 6px'>{html.escape(style.label)}</h4>",
        f"<p style='margin:0 0 6px'><strong>Ubicación:</strong> {html.escape(place) if place else 'No especificada'}</p>",
        f"<p style='margin:0 0 6px'><strong>País:</strong> {html.escape(pais) if pais else '—'}</p>",
        f"<p style='margin:0 0 6px'><strong>Descripción:</strong><br>{html.escape(descripcion)}</p>",
    ]
    if fuente_html:
        parts.append(f"<p style='margin:0 0 6px'><strong>Fuente:</strong> {fuente_html}</p>")
    if accuracy:
        parts.append(f"<p style='margin:0 0 6px'><strong>Precisión geocodificación:</strong> {html.escape(accuracy)}</p>")
    if created:
        parts.append(f"<p style='margin:0 0 4px'><strong>Creado:</strong> {html.escape(created)}</p>")
    if updated and updated != created:
        parts.append(f"<p style='margin:0 0 4px'><strong>Actualizado:</strong> {html.escape(updated)}</p>")
    return "".join(parts)


def _build_tooltip(incidente: dict, style: CategoryStyle) -> str:
    place = incidente.get("place") or incidente.get("pais") or "Sin ubicación"
    dt = _format_dt(incidente.get("created_at"))
    parts = [style.label]
    if place:
        parts.append(f"· {place}")
    if dt:
        parts.append(f"· {dt}")
    return " ".join(parts)


def _build_legend_html(styles: Iterable[CategoryStyle]) -> str:
    items = []
    for style in sorted(styles, key=lambda s: s.label):
        items.append(
            f"<li><span style='background:{style.color}'></span>{html.escape(style.label)}</li>"
        )
    items_html = "".join(items)
    legend_css = """
    <style>
    .incident-legend {
        position: fixed;
        bottom: 24px;
        left: 24px;
        z-index: 9999;
        background: rgba(255, 255, 255, 0.92);
        padding: 12px 14px;
        border-radius: 6px;
        box-shadow: 0 2px 6px rgba(0,0,0,0.25);
        font-family: "Helvetica Neue", Arial, sans-serif;
        font-size: 13px;
        line-height: 1.4;
    }
    .incident-legend h4 {
        margin: 0 0 8px;
        font-size: 14px;
    }
    .incident-legend ul {
        list-style: none;
        margin: 0;
        padding: 0;
    }
    .incident-legend li {
        display: flex;
        align-items: center;
        gap: 6px;
        margin-bottom: 4px;
        white-space: nowrap;
    }
    .incident-legend li span {
        display: inline-block;
        width: 12px;
        height: 12px;
        border-radius: 50%;
        border: 1px solid rgba(0,0,0,0.2);
    }
    </style>
    """
    legend_box = f"""
    <div class="incident-legend">
        <h4>Categorías SICU</h4>
        <ul>{items_html}</ul>
    </div>
    """
    return legend_css + legend_box


def build_incident_map(
    output_path: str | Path,
    *,
    pais: Optional[str] = None,
    categorias: Optional[Sequence[str]] = None,
    start: Optional[str | datetime] = None,
    end: Optional[str | datetime] = None,
    tiles: str = "CartoDB positron",
    show_legend: bool = True,
) -> Path:
    """
    Genera un mapa interactivo (HTML) con todos los incidentes geolocalizados.
    """
    settings = get_settings()
    init_db()
    migrate_db()

    incidents = get_incidentes_geocodificados(
        pais=pais,
        categorias=list(categorias) if categorias else None,
        start=start,
        end=end,
    )

    if not incidents:
        raise ValueError("No se encontraron incidentes geocodificados con los filtros indicados.")

    catalog: SICUCatalog = load_sicu_catalog(settings.data_dir)
    fg = FeatureGroup(name="Incidentes", show=True)
    used_styles: dict[str, CategoryStyle] = {}

    for inc in incidents:
        lat = inc.get("lat")
        lon = inc.get("lon")
        if lat is None or lon is None:
            continue
        style = catalog.resolve(inc.get("categoria"))
        used_styles[style.code] = style
        marker = folium.CircleMarker(
            location=(lat, lon),
            radius=7,
            color=style.color,
            fill=True,
            fill_color=style.color,
            fill_opacity=0.85,
            weight=1,
            tooltip=_build_tooltip(inc, style),
        )
        popup_html = _build_popup_html(inc, style)
        marker.add_child(folium.Popup(popup_html, max_width=360))
        marker.add_to(fg)

    first_point = next(((inc["lat"], inc["lon"]) for inc in incidents if inc.get("lat") and inc.get("lon")), None)
    if first_point:
        m = folium.Map(location=first_point, zoom_start=6, control_scale=True, tiles=tiles)
    else:
        m = folium.Map(location=[0, 0], zoom_start=2, control_scale=True, tiles=tiles)

    fg.add_to(m)

    bounds = [
        (inc["lat"], inc["lon"])
        for inc in incidents
        if inc.get("lat") is not None and inc.get("lon") is not None
    ]
    if bounds:
        m.fit_bounds(bounds, padding=(30, 30))

    if show_legend and used_styles:
        legend_html = _build_legend_html(used_styles.values())
        m.get_root().html.add_child(Element(legend_html))

    folium.LayerControl(collapsed=False).add_to(m)

    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    m.save(str(output_path))
    return output_path
