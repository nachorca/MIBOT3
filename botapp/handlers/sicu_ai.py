# botapp/handlers/sicu_ai.py
from __future__ import annotations

from pathlib import Path
import csv
from typing import List, Dict

from telegram import Update, InputFile
from telegram.ext import ContextTypes

from botapp.config import get_settings
from botapp.utils.incidentes_csv import _slugify_country
from botapp.handlers.sicu_full import CATEG_BASE_DIR  # reutilizamos misma ruta
from botapp.services.llm_client import generate_sicu_analysis

SET = get_settings()


async def sicu_ai(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    /sicu_ai <pais> <YYYY-MM-DD>

    Usa el CSV de incidentes SICU (ya deduplicado) para generar un informe analítico
    mediante ChatGPT y devuelve el texto.
    """
    message = update.message or update.effective_message
    args = context.args or []

    if len(args) < 2:
        return await message.reply_text(
            "Uso: /sicu_ai <pais> <YYYY-MM-DD>\n"
            "Ejemplo: /sicu_ai haiti 2025-11-25\n\n"
            "Nota: primero ejecuta /sicu_full para ese día, para que exista el CSV SICU."
        )

    raw_country = args[0].strip()
    day = args[1].strip()
    country_slug = _load_slug(raw_country)

    csv_path = CATEG_BASE_DIR / country_slug / f"{country_slug}-{day}_incidentes_SICU.csv"
    if not csv_path.exists():
        return await message.reply_text(
            f"❌ No encuentro el CSV SICU para {raw_country.upper()} {day}.\n"
            f"Busca: {csv_path}\n"
            "Primero ejecuta /sicu_full para ese día."
        )

    # Cargar incidentes del CSV
    incidents: List[Dict[str, str]] = []
    try:
        with csv_path.open(newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                incidents.append(dict(row))
    except Exception as e:
        return await message.reply_text(f"❌ Error leyendo CSV SICU: {e!r}")

    if not incidents:
        return await message.reply_text("ℹ️ El CSV SICU está vacío. No hay incidentes que analizar.")

    await message.reply_text(
        f"⏳ Generando informe analítico para {raw_country.upper()} {day} con ayuda de IA…"
    )

    # Llamar a la IA
    text = await generate_sicu_analysis(raw_country.upper(), day, incidents)

    # Si el texto parece ser un mensaje de error (empieza por ❌), lo mandamos tal cual
    if text.strip().startswith("❌"):
        return await message.reply_text(text)

    # Enviar como documento TXT para que puedas abrirlo/adjuntarlo fácilmente
    from io import BytesIO
    buf = BytesIO()
    buf.write(text.encode("utf-8"))
    buf.seek(0)

    filename = f"{country_slug}-{day}_SICU_AI.txt"
    await message.bot.send_document(
        chat_id=message.chat_id,
        document=InputFile(buf, filename=filename),
        caption=f"📄 Informe analítico SICU (IA) :: {raw_country.upper()} {day}"
    )


def _load_slug(raw_country: str) -> str:
    # reutilizamos el helper de sicu_full
    return _slugify_country(raw_country)