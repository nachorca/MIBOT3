# -*- coding: utf-8 -*-
# botapp/handlers/csv_to_kml.py
from __future__ import annotations

from pathlib import Path
from typing import List
import unicodedata

from telegram import Update, InputFile
from telegram.ext import ContextTypes, CommandHandler

from botapp.config import get_settings
from botapp.utils.csv_to_kml import csv_to_kml  # función utilitaria que genera el KML

SET = get_settings()

# Rutas base coherentes con el resto del proyecto
DATA_DIR = Path(SET.data_dir).resolve()
PROJECT_ROOT = DATA_DIR.parent if DATA_DIR.parent != DATA_DIR else Path(__file__).resolve().parents[2]
OUTPUT_DIR = PROJECT_ROOT / "output"
CATEG_BASE_DIR = OUTPUT_DIR / "incidentes_categorizados"  # donde se guardan los *_incidentes_SICU.csv


def _slugify_country(raw: str) -> str:
    """
    Normaliza el nombre de país:
      - pasa a minúsculas
      - quita tildes
      - aplica alias simples (libya -> libia, el campello -> campello, etc.)
    """
    if not raw:
        return ""
    s = raw.strip().lower()
    s_norm = "".join(
        c for c in unicodedata.normalize("NFD", s)
        if unicodedata.category(c) != "Mn"
    )
    aliases = {
        "libia": "libia",
        "libya": "libia",
        "haiti": "haiti",
        "haiti ": "haiti",
        "mali": "mali",
        "malí": "mali",
        "gaza": "gaza",
        "colombia": "colombia",
        "campello": "campello",
        "el campello": "campello",
    }
    return aliases.get(s_norm, s_norm)


async def csv_to_kml_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    /csv_to_kml <pais> <YYYY-MM-DD>

    Genera un KML a partir del CSV SICU CATEGORIZADO del país y fecha indicados.

    ➜ Usa SIEMPRE el archivo en:
        output/incidentes_categorizados/<pais>/<pais>-<fecha>_incidentes_SICU.csv

    Ejemplo concreto para LIBIA:
        output/incidentes_categorizados/libia/libia-2025-11-22_incidentes_SICU.csv
    """
    args = context.args or []
    if len(args) < 2:
        return await update.message.reply_text(
            "Uso: /csv_to_kml <pais> <YYYY-MM-DD>\n"
            "Ejemplo: /csv_to_kml libia 2025-11-22"
        )

    raw_country = args[0].strip()
    day = args[1].strip()
    country_slug = _slugify_country(raw_country)

    # Carpeta de incidentes categorizados para este país
    country_dir = CATEG_BASE_DIR / country_slug
    if not country_dir.exists():
        return await update.message.reply_text(
            "❌ No existe carpeta SICU para este país.\n"
            f"Buscado: {country_dir}"
        )

    # Archivo concreto esperado:
    #   <pais>-<fecha>_incidentes_SICU.csv
    expected_csv = country_dir / f"{country_slug}-{day}_incidentes_SICU.csv"

    if not expected_csv.exists():
        # Mostrar lo que hay para ayudarte a ver el nombre correcto
        disponibles = [p.name for p in sorted(country_dir.glob("*_incidentes_SICU.csv"))]
        disp_text = "\n".join(f"- {n}" for n in disponibles) if disponibles else "(no hay CSVs)"
        return await update.message.reply_text(
            "❌ No encontré CSV SICU para esa fecha.\n"
            f"Buscado: {expected_csv.name}\n"
            f"Carpeta: {country_dir}\n\n"
            f"Disponibles:\n{disp_text}"
        )

    await update.message.reply_text(
        f"📄 Usando CSV SICU: {expected_csv.name}\nGenerando KML…"
    )

    # Ruta de salida KML: mismo nombre, extensión .kml
    out_kml = expected_csv.with_suffix(".kml")

    try:
        # enrich=True y country=country_slug para que la utilidad pueda usar gazetteer, etc.
        kml_path_str = csv_to_kml(
            csv_path=str(expected_csv),
            out_path=str(out_kml),
            day_iso=day,
            enrich=True,
            country=country_slug,
        )
        kml_path = Path(kml_path_str)
    except Exception as e:
        return await update.message.reply_text(
            f"❌ Error generando KML desde {expected_csv.name}: {e!r}"
        )

    if not kml_path.exists():
        return await update.message.reply_text(
            f"⚠️ KML esperado en {kml_path}, pero no se encontró en disco."
        )

    # Enviar el KML al chat
    try:
        with kml_path.open("rb") as fh:
            await update.message.reply_document(
                document=InputFile(fh, filename=kml_path.name),
                caption=f"🗺️ KML SICU :: {raw_country.upper()} {day}",
            )
    except Exception as e:
        return await update.message.reply_text(
            f"✅ KML creado en {kml_path}, pero no se pudo enviar: {e!r}"
        )


def get_handlers():
    """Devuelve el CommandHandler para registrar en main.py si hiciera falta."""
    return [CommandHandler("csv_to_kml", csv_to_kml_cmd)]