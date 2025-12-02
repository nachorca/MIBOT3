from __future__ import annotations

from telegram import Update, InputFile
from telegram.ext import ContextTypes
from pathlib import Path
from io import BytesIO
import zipfile

from ..config import get_settings
from ..services.store import Store
from ..utils.date_range import dates_list, last_ndays
from ..services.report_hooks import registrar_incidentes_desde_texto
from botapp.utils.translator import to_spanish_full  # para /txtdia_es

SET = get_settings()
STORE = Store(SET.data_dir)


def _country_dir(country: str) -> Path:
    d = Path(SET.data_dir) / country.lower()
    d.mkdir(parents=True, exist_ok=True)
    return d


# ======================================================
#                /txtdia  (ORIGINAL, SIN TRADUCIR)
# ======================================================
async def txtdia(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    /txtdia <pais> <YYYY-MM-DD>

    ➜ Envía el TXT ORIGINAL del día (sin traducir).
    """
    if len(context.args) < 2:
        return await update.message.reply_text("Uso: /txtdia <pais> <YYYY-MM-DD>")

    country = context.args[0].lower().strip()
    day = context.args[1].strip()

    f = _country_dir(country) / f"{day}.txt"

    if not f.exists():
        return await update.message.reply_text(f"No hay TXT para {country.upper()} en {day}.")

    # Leer archivo original
    try:
        content = f.read_text(encoding="utf-8", errors="ignore")
    except Exception as e:
        print(f"[txtdia] error leyendo TXT: {e!r}")
        content = ""

    # Registrar incidentes desde TXT original
    if content.strip():
        ingest_country = country.replace("_", " ").strip().title()
        try:
            registrados = registrar_incidentes_desde_texto(
                pais=ingest_country,
                texto_informe=content,
                fuente=f"TXT {country.upper()} {day}",
                resolver_ahora=True,
                country_hint=ingest_country,
            )
            print(f"[txtdia] {country} {day}: {registrados} incidentes")
        except Exception as e:
            print(f"[txtdia] fallo registrando incidentes: {e!r}")

    # Enviar archivo ORIGINAL (sin traducir)
    buf = BytesIO()
    buf.write(content.encode("utf-8"))
    buf.seek(0)

    await update.message.reply_document(
        document=InputFile(buf, filename=f"{country}-{day}.txt"),
        caption=f"{country.upper()} :: {day} (TXT original)"
    )


# ======================================================
#                /txtdia_es  (TRADUCIDO AL ESPAÑOL)
# ======================================================
async def txtdia_es(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    /txtdia_es <pais> <YYYY-MM-DD>

    ➜ Envía el TXT del día traducido al ESPAÑOL.
    (No vuelve a registrar incidentes; usa solo para lectura en ES).
    """
    if len(context.args) < 2:
        return await update.message.reply_text("Uso: /txtdia_es <pais> <YYYY-MM-DD>")

    country = context.args[0].lower().strip()
    day = context.args[1].strip()

    f = _country_dir(country) / f"{day}.txt"

    if not f.exists():
        return await update.message.reply_text(f"No hay TXT para {country.upper()} en {day}.")

    # Leer archivo original
    try:
        content = f.read_text(encoding="utf-8", errors="ignore")
    except Exception as e:
        print(f"[txtdia_es] error leyendo TXT: {e!r}")
        content = ""

    # Traducir al español (con caché integrada en translator.py)
    try:
        translated = to_spanish_full(content)
    except Exception as e:
        print(f"[txtdia_es] error en traducción, envío original: {e!r}")
        translated = content or ""

    # Enviar archivo traducido
    buf = BytesIO()
    buf.write(translated.encode("utf-8"))
    buf.seek(0)

    await update.message.reply_document(
        document=InputFile(buf, filename=f"{country}-{day}-ES.txt"),
        caption=f"{country.upper()} :: {day} (traducido ES)"
    )


# ======================================================
#                /txtrango  (SIN CAMBIOS)
# ======================================================
async def txtrango(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    /txtrango <pais> <YYYY-MM-DD> <YYYY-MM-DD>
    Combina días del rango (inclusive) y envía un TXT único.
    """
    if len(context.args) < 3:
        return await update.message.reply_text("Uso: /txtrango <pais> <YYYY-MM-DD> <YYYY-MM-DD>")

    country = context.args[0].lower().strip()
    start = context.args[1].strip()
    end = context.args[2].strip()

    days = dates_list(SET.tz, start, end)
    combined = STORE.read_recent(country, days)

    if not combined.strip():
        return await update.message.reply_text(
            f"No hay contenido para {country.upper()} entre {start} y {end}."
        )

    buf = BytesIO()
    buf.write(combined.encode("utf-8"))
    buf.seek(0)

    name = f"{country}-{start}_a_{end}.txt"
    await update.message.reply_document(
        document=InputFile(buf, filename=name),
        caption=f"{country.upper()} :: {start} → {end}"
    )


# ======================================================
#                /txtsemana  (SIN CAMBIOS)
# ======================================================
async def txtsemana(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    /txtsemana <pais> [n_dias]
    Junta los últimos n días (por defecto 7) y envía un TXT.
    """
    if len(context.args) < 1:
        return await update.message.reply_text("Uso: /txtsemana <pais> [n_dias]")

    country = context.args[0].lower().strip()
    n = 7

    if len(context.args) >= 2:
        try:
            n = max(1, int(context.args[1]))
        except Exception:
            pass

    days = list(reversed(last_ndays(SET.tz, n)))
    combined = STORE.read_recent(country, days)

    if not combined.strip():
        return await update.message.reply_text(
            f"No hay contenido para {country.upper()} en últimos {n} días."
        )

    buf = BytesIO()
    buf.write(combined.encode("utf-8"))
    buf.seek(0)

    name = f"{country}-ultimos_{n}_dias.txt"
    await update.message.reply_document(
        document=InputFile(buf, filename=name),
        caption=f"{country.upper()} :: últimos {n} días"
    )


# ======================================================
#                /zipsemana  (SIN CAMBIOS)
# ======================================================
async def zipsemana(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    /zipsemana <pais> [n_dias]
    Comprime los TXT diarios de los últimos n días (por defecto 7) y envía un ZIP.
    """
    if len(context.args) < 1:
        return await update.message.reply_text("Uso: /zipsemana <pais> [n_dias]")

    country = context.args[0].lower().strip()
    n = 7

    if len(context.args) >= 2:
        try:
            n = max(1, int(context.args[1]))
        except Exception:
            pass

    days = last_ndays(SET.tz, n)
    cdir = _country_dir(country)
    files = [(cdir / f"{d}.txt") for d in days if (cdir / f"{d}.txt").exists()]

    if not files:
        return await update.message.reply_text(
            f"No hay archivos diarios para {country.upper()} en últimos {n} días."
        )

    buf = BytesIO()

    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        for f in files:
            z.write(f, arcname=f.name)

    buf.seek(0)
    zipname = f"{country}-ultimos_{n}_dias.zip"

    await update.message.reply_document(
        document=InputFile(buf, filename=zipname),
        caption=f"{country.upper()} :: ZIP {n} días"
    )