# -*- coding: utf-8 -*-
from typing import Iterable, Dict, Any, Optional
from botapp.services.incidentes_db import init_db, migrate_db, add_incidente, incidente_exists
from botapp.services.incidentes_resolver import resolve_missing_coords
from botapp.services.incident_parser import parse_incidents_from_text
from botapp.config import get_settings

# ⬇️ importar el helper de CSV
try:
    from botapp.utils.incidentes_csv import save_events_csv_from_list
except Exception:
    save_events_csv_from_list = None


def registrar_incidente_desde_informe(
    pais: str,
    categoria: str,
    descripcion: str,
    fuente: str,
    lat: float = None,
    lon: float = None,
    place: str = None,
    resolver_ahora: bool = True,
    country_hint: str = None,
) -> int:
    """
    Inserta un único incidente en la DB y actualiza el CSV del día.
    """
    init_db(); migrate_db()
    rowid = add_incidente(
        pais=pais,
        categoria=categoria,
        descripcion=descripcion,
        lat=lat,
        lon=lon,
        place=place,
        fuente=fuente,
    )
    if resolver_ahora:
        resolve_missing_coords(default_country_hint=country_hint or pais)

    # --- NUEVO: actualizar CSV con un solo incidente ---
    if save_events_csv_from_list is not None:
        try:
            from botapp.utils.operational_day import opday_today_str
            day_iso = opday_today_str(get_settings().tz)
            _csv = save_events_csv_from_list(
                country=pais.lower(),
                day_iso=day_iso,
                incidentes=[{
                    "categoria": categoria,
                    "descripcion": descripcion,
                    "lat": lat,
                    "lon": lon,
                    "place": place,
                    "fuente": fuente,
                }],
            )
            print(f"[report_hooks] CSV actualizado: {_csv}")
        except Exception as e:
            print(f"[report_hooks] aviso: no se pudo actualizar CSV (single): {e!r}")

    return rowid


def registrar_incidentes_desde_lista(
    pais: str,
    incidentes: Iterable[Dict[str, Any]],
    resolver_ahora: bool = True,
    country_hint: Optional[str] = None,
    day_iso: Optional[str] = None,   # ⬅️ NUEVO: fecha del op-day
) -> int:
    """
    Inserta una lista de incidentes en la DB y actualiza automáticamente el CSV del día.
    """
    init_db(); migrate_db()
    n = 0
    for inc in incidentes:
        categoria = inc.get("categoria") or inc.get("categoria_sicu") or "Otros"
        descripcion = (inc.get("descripcion") or "").strip()
        if not descripcion:
            continue
        place_val = inc.get("place") or inc.get("localizacion")
        if incidente_exists(
            pais=pais,
            categoria=categoria,
            descripcion=descripcion,
            place=place_val,
        ):
            continue
        fuente = inc.get("fuente") or inc.get("Fuente_URL") or "Informe Diario"
        add_incidente(
            pais=pais,
            categoria=categoria,
            descripcion=descripcion,
            lat=inc.get("lat"),
            lon=inc.get("lon"),
            place=place_val,
            fuente=fuente,
        )
        n += 1

    if resolver_ahora:
        resolve_missing_coords(default_country_hint=country_hint or pais)

    # --- NUEVO: guardar/actualizar CSV del día ---
    if save_events_csv_from_list is not None:
        try:
            from botapp.utils.operational_day import opday_today_str
            if day_iso is None:
                day_iso = opday_today_str(get_settings().tz)
            _csv = save_events_csv_from_list(
                country=pais.lower(),
                day_iso=day_iso,
                incidentes=incidentes,
            )
            print(f"[report_hooks] CSV actualizado: {_csv}")
        except Exception as e:
            print(f"[report_hooks] aviso: no se pudo actualizar CSV (lista): {e!r}")

    return n


def registrar_incidentes_desde_texto(
    pais: str,
    texto_informe: str,
    fuente: str = "Informe Diario",
    resolver_ahora: bool = True,
    country_hint: Optional[str] = None,
) -> int:
    """
    Parsea incidentes desde texto, los inserta en DB y actualiza CSV automáticamente.
    """
    incidentes = parse_incidents_from_text(texto_informe, default_fuente=fuente)
    return registrar_incidentes_desde_lista(
        pais=pais,
        incidentes=incidentes,
        resolver_ahora=resolver_ahora,
        country_hint=country_hint or pais,
    )
