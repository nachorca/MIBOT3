from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, Optional


@dataclass(frozen=True)
class CategoryStyle:
    code: str
    label: str
    color: str
    icon: Optional[str] = None
    description: Optional[str] = None


DEFAULT_PALETTE = [
    "#d73027",
    "#fc8d59",
    "#fee090",
    "#e0f3f8",
    "#91bfdb",
    "#4575b4",
    "#af8dc3",
    "#f7f7f7",
    "#7fbf7b",
    "#1a9850",
]

UNKNOWN_STYLE = CategoryStyle(
    code="SIN_CATEGORIA",
    label="Sin clasificar",
    color="#666666",
    icon=None,
    description="Incidente sin categoría SICU definida.",
)


class SICUCatalog:
    """
    Catálogo de categorías SICU -> estilos para el mapa.
    Permite definir colores/iconos personalizados mediante data/sicu_catalog.json.
    """

    def __init__(
        self,
        entries: Dict[str, CategoryStyle],
        palette: Iterable[str] = DEFAULT_PALETTE,
    ) -> None:
        self._entries = entries
        self._palette = list(palette) or DEFAULT_PALETTE
        self._fallback_cache: Dict[str, CategoryStyle] = {}

    def resolve(self, categoria: Optional[str]) -> CategoryStyle:
        if not categoria:
            return UNKNOWN_STYLE
        codigo = categoria.strip()
        if not codigo:
            return UNKNOWN_STYLE
        style = self._entries.get(codigo)
        if style:
            return style
        cached = self._fallback_cache.get(codigo)
        if cached:
            return cached
        color = self._palette[abs(hash(codigo)) % len(self._palette)]
        generated = CategoryStyle(code=codigo, label=codigo, color=color)
        self._fallback_cache[codigo] = generated
        return generated

    @property
    def entries(self) -> Dict[str, CategoryStyle]:
        return self._entries


def _load_from_json(path: Path) -> Dict[str, CategoryStyle]:
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}

    entries: Dict[str, CategoryStyle] = {}
    if isinstance(raw, dict):
        for key, value in raw.items():
            if not isinstance(value, dict):
                continue
            label = value.get("label") or key
            color = value.get("color") or DEFAULT_PALETTE[abs(hash(key)) % len(DEFAULT_PALETTE)]
            icon = value.get("icon")
            description = value.get("description")
            entries[key] = CategoryStyle(
                code=key,
                label=label,
                color=color,
                icon=icon,
                description=description,
            )
    return entries


def load_sicu_catalog(data_dir: str | Path) -> SICUCatalog:
    """
    Carga el catálogo SICU desde data/sicu_catalog.json si existe.
    Si no, genera un catálogo vacío con paleta por defecto.
    """
    data_path = Path(data_dir)
    path = data_path / "sicu_catalog.json"
    entries = _load_from_json(path) if path.exists() else {}
    return SICUCatalog(entries)
