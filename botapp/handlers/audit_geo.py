# -*- coding: utf-8 -*-
from __future__ import annotations
from pathlib import Path
from telegram import Update
from telegram.ext import ContextTypes, CommandHandler
from botapp.utils.audit_geo import audit_csv

OUTPUT_DIR = Path("/Users/joseignaciosantiagomartin/curso de python/MIBOT3/output")
INCIDENTS_DIR = OUTPUT_DIR / "incidentes"

async def audit_csv_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    /audit_csv <pais> <YYYY-MM-DD>
    Muestra cuántas filas tienen Lat/Lon válidas y ejemplos de problemas.
    """
    args = context.args or []
    if len(args) < 2:
        return await update.message.reply_text("Uso: /audit_csv <pais> <YYYY-MM-DD>")
    country = args[0].strip().lower()
    day = args[1].strip()

    p = INCIDENTS_DIR / f"incidentes_{country}_{day}.csv"
    if not p.exists():
        return await update.message.reply_text(f"❌ No existe: {p}")

    total, valid, missing, no_coord, bad = audit_csv(p)
    lines = [
        f"📊 Auditoría CSV {country.upper()} {day}",
        f"Total filas: {total}",
        f"Con coord válidas: {valid}",
        f"Sin coord: {missing}",
    ]
    if no_coord:
        lines.append("\nEjemplos sin coord:")
        for i, loc, desc in no_coord:
            lines.append(f" - fila {i}: loc='{loc}' desc='{desc}'")
    if bad:
        lines.append("\nEjemplos coord no parseables:")
        for i, la, lo in bad:
            lines.append(f" - fila {i}: Lat='{la}' Lon='{lo}'")
    await update.message.reply_text("\n".join(lines))

def get_handlers():
    return [CommandHandler("audit_csv", audit_csv_cmd)]