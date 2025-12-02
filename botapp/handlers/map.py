# -*- coding: utf-8 -*-
from __future__ import annotations
from telegram import Update
from telegram.ext import ContextTypes
from botapp.services.map_builder import build_incidents_map

HELP_TXT = (
    "Uso: /map_incidentes [pais] [dias]\n"
    " - pais: (opcional) ej. Libia, Haití, Campello, Colombia\n"
    " - dias: (opcional) número de días hacia atrás, por defecto 7\n"
    "Ejemplos:\n"
    "  /map_incidentes\n"
    "  /map_incidentes Libia\n"
    "  /map_incidentes Haiti 3\n"
)

async def map_incidentes(update: Update, context: ContextTypes.DEFAULT_TYPE):
    args = context.args or []
    pais = None
    days = 7
    if len(args) >= 1:
        if args[0].isdigit():
            days = int(args[0])
        else:
            pais = args[0]
    if len(args) >= 2:
        try:
            days = int(args[1])
        except Exception:
            pass

    try:
        await update.message.reply_text("⏳ Generando mapa de incidentes… (esto puede llevar unos segundos)")
        outpath = build_incidents_map(pais=pais, days=days, resolve_missing=True)
        with open(outpath, "rb") as f:
            await context.bot.send_document(chat_id=update.effective_chat.id, document=f, filename=outpath.split("/")[-1],
                                            caption=f"Mapa de incidentes ({pais or 'todos'}) últimos {days} días.")
    except Exception as e:
        await update.message.reply_text(f"❗ No se pudo generar el mapa. {e}\n{HELP_TXT}")
