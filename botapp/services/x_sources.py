# botapp/services/x_sources.py
from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, List

# Raíz de MIBOT3 → /data/x_sources.json
BASE_DIR = Path(__file__).resolve().parents[2]
X_SOURCES_PATH = BASE_DIR / "data" / "x_sources.json"


def load_x_sources() -> Dict[str, List[str]]:
    """
    Carga el JSON de fuentes de Twitter/X por país.

    Estructura esperada:
    {
      "haiti": ["@HaitiLibre", "@machannzen", ...],
      "libia": [...],
      ...
    }
    """
    if not X_SOURCES_PATH.exists():
        return {}
    with X_SOURCES_PATH.open("r", encoding="utf-8") as f:
        return json.load(f)


X_SOURCES: Dict[str, List[str]] = load_x_sources()


def get_x_sources(country: str) -> List[str]:
    """
    Devuelve la lista de handles de Twitter/X para un país dado.

    Ejemplo: get_x_sources("haiti") -> ["@HaitiLibre", "@machannzen", ...]
    """
    country = country.lower()
    return X_SOURCES.get(country, [])