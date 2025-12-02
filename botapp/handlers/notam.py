from __future__ import annotations
from telegram import Update
from telegram.ext import ContextTypes
from ..utils.notam_header import prepend_notam_header

async def notam(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    /notam <ICAO> <pais>
    Inserta el bloque NOTAM (para ese ICAO) al inicio del TXT del día operativo del país.
    Ej: /notam MTPP haiti
        /notam HLLM libia
    """
    if len(context.args) < 2:
        return await update.message.reply_text("Uso: /notam <ICAO> <pais>  (ej. /notam MTPP haiti)")

    icao = context.args[0].upper().strip()
    country = context.args[1].lower().strip()

    try:
        fpath = await prepend_notam_header(icao, country)
        await update.message.reply_text(f"✅ Bloque NOTAM {icao} añadido en {fpath.name}. Usa /txt {country} para ver el fichero.")
    except Exception as e:
        await update.message.reply_text(f"❗ Error añadiendo NOTAM {icao}: {e}")