# -*- coding: utf-8 -*-
import json
import time
import urllib.parse
from typing import Iterable, Optional, Tuple

import re
import urllib.error
import urllib.request

from botapp.services.incidentes_db import geocache_get, geocache_put

# Activa/desactiva geocodificador online (si tu servidor no tiene salida a Internet, pon False)
USE_ONLINE_GEOCODER = True
USER_AGENT = "MIBOT3/1.0 (contact: info@santiagolegalconsulting.es)"

_WS_RE = re.compile(r"\s+")
_PARENS_RE = re.compile(r"\(([^)]+)\)")
_DIRECTION_RE = re.compile(
    r"\b(?:al|a la|a los|a las|towards|north of|south of|east of|west of|noreste de|noroeste de|"
    r"norte de|sur de|este de|oeste de|noreste|noroeste|sureste|suroeste)\b",
    re.IGNORECASE,
)
_NEAR_RE = re.compile(
    r"\b(?:cerca de|en las cercan[ií]as de|en las proximidades de|pr[oó]ximo a|alrededor de|near|around|"
    r"adjacent to|junto a|junto al|junto a la)\b",
    re.IGNORECASE,
)
_TRAILING_QUALIFIERS_RE = re.compile(
    r"(?:\b(?:city|ciudad|province|provincia|state|estado|region|región|district|distrito|governorate)\b\.?)$",
    re.IGNORECASE,
)
_COUNTRY_ALIASES = {
    "libia": "Libya",
    "libya": "Libya",
    "haiti": "Haiti",
    "haití": "Haiti",
    "colombia": "Colombia",
    "campello": "Spain",
    "españa": "Spain",
    "spain": "Spain",
    "gaza": "Gaza Strip",
    "gaza strip": "Gaza Strip",
    "palestine": "State of Palestine",
    "palestina": "State of Palestine",
    "state of palestine": "State of Palestine",
    "liberia": "Liberia",
}


def _canonical_country(country: Optional[str]) -> Optional[str]:
    if not country:
        return None
    norm = country.strip().lower()
    if not norm:
        return None
    mapped = _COUNTRY_ALIASES.get(norm)
    if mapped:
        return mapped
    return country.strip()


def _sanitize_place(place: str) -> str:
    cleaned = place.strip().strip(",.;")
    cleaned = cleaned.replace("#", " ")
    cleaned = cleaned.replace("•", " ").replace("●", " ")
    cleaned = _DIRECTION_RE.sub("", cleaned)
    cleaned = _NEAR_RE.sub("", cleaned)
    cleaned = _WS_RE.sub(" ", cleaned).strip(" ,;:-")
    cleaned = _TRAILING_QUALIFIERS_RE.sub("", cleaned).strip(" ,;:-")
    return cleaned


def _iter_alt_tokens(place: str) -> Iterable[str]:
    parens = _PARENS_RE.findall(place)
    for chunk in parens:
        chunk = _sanitize_place(chunk)
        if chunk:
            yield chunk
    if "," in place:
        head = _sanitize_place(place.split(",", 1)[0])
        tail = _sanitize_place(place.split(",", 1)[-1])
        if head:
            yield head
        if tail and tail != head:
            yield tail
    if "/" in place:
        for piece in place.split("/"):
            piece = _sanitize_place(piece)
            if piece:
                yield piece


def _build_queries(place: str, country: Optional[str]) -> list[str]:
    base = _sanitize_place(place)
    queries: list[str] = []
    seen: set[str] = set()

    def _add(candidate: str) -> None:
        c = candidate.strip(", ")
        if not c:
            return
        canon = c.lower()
        if canon in seen:
            return
        seen.add(canon)
        queries.append(c)

    if base:
        _add(base)
        if country:
            _add(f"{base}, {country}")

    for extra in _iter_alt_tokens(place):
        _add(extra)
        if country:
            _add(f"{extra}, {country}")

    if not queries and country:
        _add(country)

    return queries


def _cache_key(place: str, country: Optional[str]) -> str:
    canonical_country = _canonical_country(country)
    sanitized_place = _sanitize_place(place).lower()
    return f"{sanitized_place}||{(canonical_country or '').lower()}"


def _nominatim_search(query: str) -> Optional[dict]:
    params = {
        "q": query,
        "format": "json",
        "limit": 1,
        "addressdetails": 1,
    }
    url = "https://nominatim.openstreetmap.org/search?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=15) as resp:
        data = json.loads(resp.read().decode("utf-8", errors="ignore"))
    if not data:
        return None
    return data[0]


def geocode_place(place: str, country: Optional[str] = None) -> Optional[Tuple[float, float, Optional[str], Optional[str], Optional[str], str]]:
    """
    Devuelve (lat, lon, admin1, admin2, accuracy, source) o None si no resuelve.
    source ∈ {'cache','nominatim'}
    """
    if not place or not place.strip():
        return None

    canonical_country = _canonical_country(country)
    key = _cache_key(place, canonical_country)
    cached = geocache_get(key)
    if cached:
        lat, lon, _country, admin1, admin2, accuracy = cached
        return (lat, lon, admin1, admin2, accuracy, "cache")

    if not USE_ONLINE_GEOCODER:
        return None

    queries = _build_queries(place, canonical_country)
    if not queries:
        return None

    for idx, query in enumerate(queries):
        # Rate-limit básico: 1 req/seg (Nominatim lo exige). Si haces lotes, añade sleeps fuera.
        if idx == 0:
            time.sleep(1.05)
        else:
            time.sleep(1.05)
        try:
            item = _nominatim_search(query)
        except urllib.error.HTTPError as e:
            if e.code in (429, 503):
                time.sleep(5)
                continue
            return None
        except Exception:
            continue

        if not item:
            continue

        try:
            lat = float(item["lat"])
            lon = float(item["lon"])
        except (KeyError, ValueError):
            continue

        addr = item.get("address", {})
        admin1 = addr.get("state") or addr.get("region")
        admin2 = addr.get("county") or addr.get("city_district") or addr.get("municipality") or addr.get("city")
        accuracy = item.get("type")
        geocache_put(key, lat, lon, canonical_country, admin1, admin2, accuracy, source="nominatim")
        return (lat, lon, admin1, admin2, accuracy, "nominatim")

    return None
