from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Dict, Iterable

from botapp.services.incidentes_db import registrar_incidente_desde_informe


def _iter_entries(path: Path) -> Iterable[Dict[str, Any]]:
    if not path.exists():
        raise FileNotFoundError(path)

    if path.suffix.lower() in {".jsonl", ".ndjson"}:
        with path.open("r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                yield json.loads(line)
        return

    data = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(data, list):
        for item in data:
            if isinstance(item, dict):
                yield item
        return
    if isinstance(data, dict):
        yield data


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Importa incidentes en la BD desde un archivo JSON/JSONL."
    )
    parser.add_argument(
        "file",
        type=Path,
        help="Archivo .json, .jsonl o .ndjson con incidentes.",
    )
    parser.add_argument(
        "--country-hint",
        help="Hint de país por defecto para la geocodificación si no se indica en la entrada.",
    )
    parser.add_argument(
        "--no-geocode",
        action="store_true",
        help="Si se indica, no se intenta geocodificar automáticamente tras cada inserción.",
    )

    args = parser.parse_args()
    count = 0
    for entry in _iter_entries(args.file):
        pais = (entry.get("pais") or "").strip()
        categoria = (entry.get("categoria") or "").strip()
        descripcion = (entry.get("descripcion") or "").strip()
        fuente = (entry.get("fuente") or "").strip()

        if not pais or not categoria or not descripcion:
            print("⚠️ Entrada omitida: requiere campos 'pais', 'categoria' y 'descripcion'.")
            continue

        try:
            inc_id = registrar_incidente_desde_informe(
                pais=pais,
                categoria=categoria,
                descripcion=descripcion,
                fuente=fuente,
                lat=entry.get("lat"),
                lon=entry.get("lon"),
                place=entry.get("place"),
                resolver_ahora=not args.no_geocode,
                country_hint=entry.get("country_hint") or args.country_hint,
            )
        except Exception as exc:
            print(f"❌ Error importando incidente: {exc}")
            continue
        count += 1
        print(f"✅ Incidente {inc_id} importado ({entry.get('categoria')})")

    print(f"Importación finalizada. Total insertados: {count}")


if __name__ == "__main__":
    main()
