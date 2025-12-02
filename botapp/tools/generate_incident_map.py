from __future__ import annotations

import argparse
from datetime import datetime
from pathlib import Path
from typing import List, Optional

from botapp.services.incidentes_map import build_incident_map


def _parse_datetime(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except Exception as exc:
        raise argparse.ArgumentTypeError(
            f"Fecha inválida '{value}'. Usa formato ISO 8601 (ej. 2025-10-15T07:30:00)."
        ) from exc


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Genera un mapa HTML con los incidentes geolocalizados."
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("data") / "maps" / "incidentes.html",
        help="Ruta del archivo HTML de salida (default: data/maps/incidentes.html)",
    )
    parser.add_argument("--pais", help="Filtra por país (coincidencia exacta).")
    parser.add_argument(
        "--categoria",
        action="append",
        dest="categorias",
        help="Añade un filtro por categoría SICU (puedes usar múltiples --categoria).",
    )
    parser.add_argument(
        "--start",
        type=_parse_datetime,
        help="Fecha/hora inicial en ISO 8601 (ej: 2025-10-15T07:00:00).",
    )
    parser.add_argument(
        "--end",
        type=_parse_datetime,
        help="Fecha/hora final en ISO 8601 (ej: 2025-10-16T06:59:59).",
    )
    parser.add_argument(
        "--tiles",
        default="CartoDB positron",
        help="Nombre de tiles para Folium (ej: 'OpenStreetMap', 'CartoDB positron').",
    )
    parser.add_argument(
        "--no-legend",
        action="store_true",
        help="Si se indica, no añade la leyenda de categorías al mapa.",
    )

    args = parser.parse_args()

    try:
        output = build_incident_map(
            output_path=args.output,
            pais=args.pais,
            categorias=args.categorias,
            start=args.start,
            end=args.end,
            tiles=args.tiles,
            show_legend=not args.no_legend,
        )
    except ValueError as exc:
        raise SystemExit(str(exc)) from exc

    print(f"Mapa generado en: {output}")


if __name__ == "__main__":
    main()
