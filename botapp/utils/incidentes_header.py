from __future__ import annotations

from datetime import timedelta, timezone
from pathlib import Path
import re
from typing import Dict, List

from ..config import get_settings
from ..services.incidentes_db import (
    get_incidentes,
    init_db,
    migrate_db,
)
from ..services.incidentes_styles import load_sicu_catalog
from ..services.incident_parser import parse_incidents_from_text
from ..services.report_hooks import registrar_incidentes_desde_lista
from ..utils.operational_day import opday_bounds, opday_today_str

SETTINGS = get_settings()

EVENTOS_START = r"^=== EVENTOS .+ ===$"
EVENTOS_END = r"^=== FIN EVENTOS ===$"
EVENTOS_BLOCK_RE = re.compile(
    r"^=== EVENTOS .+ ===$\n.*?^=== FIN EVENTOS ===$\n?",
    flags=re.MULTILINE | re.DOTALL,
)
OLD_SUCESOS_BLOCK_RE = re.compile(
    r"^=== SUCESOS .+ ===$\n.*?^=== FIN SUCESOS ===$\n?",
    flags=re.MULTILINE | re.DOTALL,
)


def _today_file(country: str, opday: str | None = None) -> Path:
    """
    Devuelve la ruta del TXT correspondiente al día operativo indicado.
    """
    day = opday or opday_today_str(SETTINGS.tz)
    d = Path(SETTINGS.data_dir) / country.lower()
    d.mkdir(parents=True, exist_ok=True)
    return d / f"{day}.txt"


def _has_eventos_block(text: str) -> bool:
    start = re.search(EVENTOS_START, text, flags=re.MULTILINE)
    end = re.search(EVENTOS_END, text, flags=re.MULTILINE)
    return bool(start and end and start.start() < end.start())


def _opday_utc_window(opday: str):
    start_local, end_local = opday_bounds(SETTINGS.tz, opday)
    start_utc = start_local.astimezone(timezone.utc)
    end_utc = (end_local - timedelta(seconds=1)).astimezone(timezone.utc)
    start_iso = start_utc.strftime("%Y-%m-%dT%H:%M:%S")
    end_iso = end_utc.strftime("%Y-%m-%dT%H:%M:%S")
    return start_iso, end_iso


def _resolve_location(incidente: Dict) -> str:
    for key in ("place", "admin1", "admin2"):
        val = incidente.get(key)
        if val:
            return str(val).strip()
    lat = incidente.get("lat")
    lon = incidente.get("lon")
    try:
        if lat is not None and lon is not None:
            lat_f = float(lat)
            lon_f = float(lon)
            return f"{lat_f:.4f}, {lon_f:.4f}"
    except Exception:
        pass
    pais = incidente.get("pais")
    if pais:
        return str(pais).strip()
    return "Localización no especificada"


def _build_block(country: str, opday: str) -> str:
    init_db()
    migrate_db()
    start_iso, end_iso = _opday_utc_window(opday)
    rows = get_incidentes(
        pais=country,
        include_without_coords=True,
        start=start_iso,
        end=end_iso,
        order_desc=False,
    )

    catalog = load_sicu_catalog(SETTINGS.data_dir)
    header = f"=== EVENTOS {country.upper()} {opday} (SICU) ==="
    lines: List[str] = [header]

    if not rows:
        lines.append("Sin eventos registrados para este día operativo.")
    else:
        for idx, inc in enumerate(rows, start=1):
            style = catalog.resolve(inc.get("categoria"))
            desc = (inc.get("descripcion") or "").strip() or "Sin descripción disponible."
            loc = _resolve_location(inc)
            lines.append(f"{idx}. Localización: {loc}")
            lines.append(f"   Categoría SICU: {style.label} [{style.code}]")
            lines.append(f"   Resumen: {desc}")
            lines.append("")

        if lines and lines[-1] == "":
            lines.pop()

    lines.append("=== FIN EVENTOS ===")
    lines.append("")  # Línea en blanco final
    return "\n".join(lines)


async def prepend_incidents_header(country: str, opday: str | None = None) -> Path:
    """
    Asegura que el TXT del día operativo incluye un bloque 'Eventos' al inicio.
    """
    opday_str = opday or opday_today_str(SETTINGS.tz)
    fpath = _today_file(country, opday_str)
    if not fpath.exists():
        fpath.write_text("", encoding="utf-8")

    content = fpath.read_text(encoding="utf-8")
    parse_target = content
    if _has_eventos_block(parse_target):
        parse_target = EVENTOS_BLOCK_RE.sub("", parse_target, count=1)
    if OLD_SUCESOS_BLOCK_RE.search(parse_target):
        parse_target = OLD_SUCESOS_BLOCK_RE.sub("", parse_target, count=1)

    try:
        incidentes = parse_incidents_from_text(parse_target, default_fuente="TXT Diario")
    except Exception:
        incidentes = []

    if incidentes:
        pais_registro = country.upper().capitalize()
        try:
            registrar_incidentes_desde_lista(
                pais=pais_registro,
                incidentes=incidentes,
                resolver_ahora=True,
                country_hint=pais_registro,
            )
        except Exception:
            pass

    remainder = content
    if _has_eventos_block(remainder):
        remainder = EVENTOS_BLOCK_RE.sub("", remainder, count=1)
    if OLD_SUCESOS_BLOCK_RE.search(remainder):
        remainder = OLD_SUCESOS_BLOCK_RE.sub("", remainder, count=1)

    block = _build_block(country, opday_str)
    updated = (block + remainder) if remainder else block
    fpath.write_text(updated, encoding="utf-8")
    return fpath
