from telegram import Update, InputFile
from telegram.ext import ContextTypes
from ..config import get_settings
from ..utils.time import today_str, dt_str
from ..services.store import Store
from ..utils.meteo_header import prepend_weather_header
from ..utils.exchange_header import prepend_exchange_header
from ..utils.incidentes_header import prepend_incidents_header
from pathlib import Path
from ..utils.operational_day import opday_today_str

from io import BytesIO  # agregado para buffer
from botapp.services.incident_parser import parse_incidents_from_text  # importar parser

SETTINGS = get_settings()
STORE = Store(SETTINGS.data_dir)

async def txt_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    /txt <pais>
    Envía el TXT del día del país. Si no existe o no tiene METEO, lo crea/añade automáticamente.
    """
    if not context.args:
        return await update.message.reply_text("Uso: /txt <pais>  (ej. /txt libia)")
    country = context.args[0].lower().strip()
    day = today_str(SETTINGS.tz)

    # Asegurar METEO, EXCHANGE y EVENTOS al inicio del archivo
    fpath = await prepend_weather_header(country)
    fpath = await prepend_exchange_header(country)
    fpath = await prepend_incidents_header(country)

    # 👇 Ordenar por hora de entrada y canal
    STORE.reorder_file(fpath)

    # Intentar insertar sección “Sucesos / Incidentes”
    try:
        text = Path(fpath).read_text(encoding="utf-8", errors="ignore")
        incidentes = parse_incidents_from_text(text, default_fuente="TXT Reporte")
        print(f"[txt_cmd] incidentes detectados: {incidentes!r}")

        if incidentes:
            bloques = ["Sucesos / Incidentes:\n"]
            cat_map = {}
            for inc in incidentes:
                cat = inc.get("categoria", "Otros")
                cat_map.setdefault(cat, []).append(inc)
            for cat, lista in cat_map.items():
                bloques.append(f"{cat}:")
                for inc in lista:
                    desc = inc.get("descripcion", "").strip()
                    place = inc.get("place") or "Localización no especificada"
                    bloques.append(f" - {desc} → {place}")
                bloques.append("")
            bloque_sucesos = "\n".join(bloques)
            nuevo_text = bloque_sucesos + "\n" + text

            buf = BytesIO()
            buf.write(nuevo_text.encode("utf-8"))
            buf.seek(0)
            await update.message.reply_document(
                document=InputFile(buf, filename=f"{country}-{day}.txt"),
                caption=f"TXT {country.upper()} :: {day}"
            )
            return
    except Exception as e:
        print(f"[txt_cmd] error generando sección de sucesos: {e!r}")

    # Si no hubo incidentes o falló, enviar el archivo original
    with open(fpath, "rb") as fh:
        await update.message.reply_document(
            document=InputFile(fh, filename=f"{country}-{day}.txt"),
            caption=f"TXT {country.upper()} :: {day}"
        )

async def add_test_entry(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    /add_test_entry <pais> <titulo> | <texto>
    Añade una entrada de prueba al TXT del día.
    """
    raw = " ".join(context.args)
    if "|" not in raw or not raw.strip():
        return await update.message.reply_text("Uso: /add_test_entry <pais> <titulo> | <texto>")
    left, text = raw.split("|", 1)
    parts = left.strip().split(maxsplit=1)
    if len(parts) < 2:
        return await update.message.reply_text("Falta <pais> y <titulo>")
    country = parts[0].lower().strip()
    title = parts[1].strip()

    day = opday_today_str(SETTINGS.tz)
    fpath = STORE.append_entry(
        country=country, day=day, title=title, dt=dt_str(SETTINGS.tz), text=text.strip()
    )
    await update.message.reply_text(f"Añadido en {fpath}")