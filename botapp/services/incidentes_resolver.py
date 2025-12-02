# -*- coding: utf-8 -*-
from typing import Optional
from botapp.services.incidentes_db import (
    init_db, migrate_db, get_incidentes_pendientes, update_incidente_geocode
)
from botapp.services.geocoder import geocode_place

def resolve_missing_coords(default_country_hint: Optional[str] = None) -> int:
    """
    Geocodifica incidentes con place pero sin lat/lon.
    default_country_hint: 'Libia'/'Haití'/... si no guardaste 'pais' como país ISO/ES-Largo.
    Retorna cuántos se resolvieron.
    """
    init_db(); migrate_db()
    pendientes = get_incidentes_pendientes()
    count = 0
    for inc in pendientes:
        place = inc.get("place")
        pais = inc.get("pais") or default_country_hint
        if not place:
            continue
        res = geocode_place(place, pais)
        if not res:
            continue
        lat, lon, admin1, admin2, acc, src = res
        update_incidente_geocode(inc["id"], lat, lon, admin1, admin2, acc, src)
        count += 1
    return count