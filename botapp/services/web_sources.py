# botapp/services/web_sources.py
from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, List


BASE_DIR = Path(__file__).resolve().parents[2]
WEB_SOURCES_PATH = BASE_DIR / "data" / "web_sources.json"


def load_web_sources() -> Dict[str, List[str]]:
    """
    Carga el JSON de fuentes web (HTTP/HTTPS) por país.

    Estructura esperada:
    {
      "haiti": ["https://www.haitilibre.com", ...],
      "libia": [...],
      ...
    }
    """
    if not WEB_SOURCES_PATH.exists():
        return {}
    try:
        data = json.loads(WEB_SOURCES_PATH.read_text(encoding="utf-8"))
    except Exception:
        data = {}
    # Normalizar claves a minúsculas
    normalized: Dict[str, List[str]] = {}
    for k, v in data.items():
        normalized[str(k).lower()] = list(v or [])
    return normalized


WEB_SOURCES: Dict[str, List[str]] = load_web_sources()


def get_web_sources(country: str) -> List[str]:
    """
    Devuelve la lista de URLs para un país dado.
    Ej: get_web_sources("haiti") -> ["https://www.haitilibre.com", ...]
    """
    country = (country or "").lower().strip()
    return WEB_SOURCES.get(country, [])