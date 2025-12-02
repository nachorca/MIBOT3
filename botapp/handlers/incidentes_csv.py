# -*- coding: utf-8 -*-
# botapp/handlers/incidentes_csv.py
from __future__ import annotations
from pathlib import Path
import csv

from telegram import Update, InputFile
from telegram.ext import ContextTypes

from botapp.config import get_settings
from botapp.utils.operational_day import opday_today_str
from botapp.utils.incidentes_csv import save_incidentes_csv_from_txt, _slugify_country  # usamos el helper

SET = get_settings()
DATA_DIR = Path(SET.data_dir).resolve()
PROJECT_ROOT = DATA_DIR.parent if DATA_DIR.parent != DATA_DIR else Path(__file__).resolve().parents[2]

def _rel_path(csv_path: Path) -> Path | str:
    try:
        return csv_path.relative_to(PROJECT_ROOT)
    except Exception:
        return csv_path

async def incidentes_csv(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    /incidentes_csv <pais> [YYYY-MM-DD]

    - Lee data/<pais>/<YYYY-MM-DD>.txt
    - Crea/actualiza output/incidentes/<pais>/incidentes_<pais>_<YYYY-MM-DD>.csv
    - Un evento por cada noticia del TXT (bloque '--- canal @ fecha hora ---').
    - Categoriza cada evento según SICU (Conflicto, Terrorismo, Criminalidad, Disturbios, Hazards, Otros).
    - Deduplica eventos ya existentes en el CSV (no duplica noticias antiguas).
    """
    args = context.args or []
    if len(args) < 1:
        return await update.message.reply_text(
            "Uso: /incidentes_csv <pais> [YYYY-MM-DD]\n"
            "Ejemplo: /incidentes_csv libia 2025-11-15"
        )

    raw_country = args[0].strip()
    country_slug = _slugify_country(raw_country)

    if len(args) >= 2:
        day = args[1].strip()
    else:
        day = opday_today_str(SET.tz)

    # Generar/actualizar CSV desde TXT
    csv_path, total = save_incidentes_csv_from_txt(country_slug, day)

    resumen = (
        f"📄 Incidentes exportados ({total} registros)\n"
        f"País: {raw_country.upper()}\n"
        f"Día: {day}\n"
        f"Ruta: {_rel_path(csv_path)}"
    )

    try:
        with csv_path.open("rb") as f:
            await update.message.reply_document(
                document=InputFile(f, filename=csv_path.name),
                caption=resumen,
            )
    except Exception as e:
        await update.message.reply_text(
            f"{resumen}\n⚠️ No se pudo adjuntar el archivo: {e!r}"
        )