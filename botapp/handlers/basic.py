from telegram import Update
from telegram.ext import ContextTypes
from ..config import get_settings
from ..utils.time import dt_str

SETTINGS = get_settings()

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(
        "🤖 Bienvenido. Bot base listo.\n"
        "Comandos: /help, /ping, /txt <pais>, /status"
    )

async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(
        "Ayuda:\n"
        " • /ping – latido del bot\n"
        " • /txt <pais> – devuelve el TXT del día para ese país (si existe)\n"
        " • /status – info rápida del bot\n"
        "Más adelante añadiremos recolectores, meteo, y reportes automáticos."
    )

async def ping(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(f"Pong! {dt_str(SETTINGS.tz)}")

async def status(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    countries = ", ".join(SETTINGS.default_countries) if SETTINGS.default_countries else "—"
    await update.message.reply_text(
        "✅ Estado: OK\n"
        f"TZ: {SETTINGS.tz}\n"
        f"DATA_DIR: {SETTINGS.data_dir}\n"
        f"Países por defecto: {countries}"
    )