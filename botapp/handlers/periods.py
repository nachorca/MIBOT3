from __future__ import annotations

from telegram import Update, InputFile
from telegram.ext import ContextTypes
from io import BytesIO
from zipfile import ZipFile, ZIP_DEFLATED
from datetime import datetime, timedelta
from pathlib import Path
import re  # 👈 para detectar eventos y bullets

from ..config import get_settings
from ..utils.operational_day import opday_bounds, opday_list, last_n_opdays
from ..services.report_reader import read_country_window
from ..utils.meteo_header import prepend_weather_header
from ..utils.exchange_header import prepend_exchange_header
from ..utils.incidentes_header import prepend_incidents_header

# ⬇️ CSV automático desde la DB (si el módulo existe; si no, no bloquea)
try:
    from ..utils.incidentes_csv import save_events_csv_from_db
except Exception:
    save_events_csv_from_db = None

# ⬇️ Traductor a inglés (opcional)
try:
    from ..utils.translator import translate_to_en
except Exception:
    translate_to_en = None

SET = get_settings()

def _to_en(text: str) -> str:
    """
    Traduce el informe al inglés si hay traductor disponible.
    Si no existe o falla, devuelve el texto original.
    """
    if not text:
        return ""
    if translate_to_en is None:
        return text
    try:
        return translate_to_en(text)
    except Exception as e:
        print(f"[report_dia] aviso: fallo traduciendo a inglés: {e!r}")
        return text

def _clean_txt_structure(text: str) -> str:
    """
    Limpia y hace más legible el TXT:
      - Añade una línea en blanco antes y después de cada bloque '=== ... ==='.
      - Deja espacio entre eventos (líneas que empiezan por 'N.' ) y bullets ('- ', '* ', '• ', '— ').
      - Evita apelotonar bloques seguidos.
    """
    lines = text.splitlines()
    out: list[str] = []
    prev_nonempty = ""

    for line in lines:
        l = line.rstrip()

        # Bloques tipo '=== EVENTOS LIBIA 2025-11-15 (SICU) ==='
        if l.startswith("===") and l.endswith("==="):
            if out and out[-1] != "":
                out.append("")
            out.append(l)
            out.append("")
            prev_nonempty = ""
            continue

        # Eventos: líneas que empiezan por "1.", "2.", etc.
        is_event = bool(re.match(r"^\d+\.", l.strip()))
        # Noticias / bullets (según tu esquema habitual)
        is_bullet = l.startswith(("- ", "* ", "• ", "— "))

        if (is_event or is_bullet) and out and out[-1] != "":
            out.append("")

        out.append(l)
        if l.strip():
            prev_nonempty = l

    cleaned = "\n".join(out).strip() + "\n"
    return cleaned

async def _send_txt(update: Update, name: str, content: str, caption: str):
    buf = BytesIO()
    buf.write(content.encode("utf-8"))
    buf.seek(0)
    await update.message.reply_document(document=InputFile(buf, filename=name), caption=caption)

# ---------- DÍA ----------
async def report_dia(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    /reportdia <pais> <YYYY-MM-DD>
    Descarga el 'día operativo' (07:00–06:59 del día siguiente) con apartado de Eventos SICU,
    guarda automáticamente el CSV normalizado (SICU) en output/incidentes/ (desde la DB)
    y exporta el TXT en inglés (si hay traductor disponible) con una estructura más limpia.
    """
    if len(context.args) < 2:
        return await update.message.reply_text("Uso: /reportdia <pais> <YYYY-MM-DD>")
    country = context.args[0].lower().strip()
    day = context.args[1].strip()

    start, end = opday_bounds(SET.tz, day)
    content = read_country_window(SET.data_dir, country, start, end)
    if not content.strip():
        return await update.message.reply_text(f"Sin contenido para {country.upper()} en {day} (07:00–06:59).")

    # (Opcional) Log mínimo
    try:
        from botapp.services.incident_parser import parse_incidents_from_text
        incidentes = parse_incidents_from_text(content, default_fuente="Reporte Día Operativo")
        print(f"[report_dia] eventos detectados en bruto ({len(incidentes)}).")
    except Exception as e:
        print(f"[report_dia] aviso: parser texto: {e!r}")

    # 1) Añadir cabeceras METEO, EXCHANGE y EVENTOS (esto registra/incorpora incidentes a la DB)
    try:
        fpath = await prepend_weather_header(country)
        fpath = await prepend_exchange_header(country)
        fpath = await prepend_incidents_header(country, opday=day)
        final_text = Path(fpath).read_text(encoding="utf-8")
    except Exception as e:
        print(f"[report_dia] error METEO/EXCHANGE/EVENTOS: {e!r}")
        final_text = content  # fallback

    # 2) Guardar también el CSV normalizado (SICU) desde la DB
    if save_events_csv_from_db is not None:
        try:
            _csv_path = save_events_csv_from_db(
                country=country,
                day_iso=day,
                tz_name=SET.tz,
            )
            print(f"[report_dia] CSV incidentes actualizado desde DB: {_csv_path}")
        except Exception as e:
            print(f"[report_dia] fallo guardando CSV incidentes (DB): {e!r}")
    else:
        print("[report_dia] aviso: utils.incidentes_csv.save_events_csv_from_db no disponible; salto CSV.")

    # 2.1) Limpiar estructura del TXT para que sea más legible
    final_text = _clean_txt_structure(final_text)

    # 2.2) Traducir TODO el texto del informe al inglés (si hay traductor)
    final_text_en = _to_en(final_text)

    # 3) Construir y enviar archivo final (en inglés)
    name = f"{country}-{day}_opday.txt"
    caption = f"{country.upper()} :: {day} (07:00–06:59) [EN]"

    buf = BytesIO()
    buf.write(final_text_en.encode("utf-8"))
    buf.seek(0)

    await update.message.reply_document(document=InputFile(buf, filename=name), caption=caption)

# ---------- SEMANA ----------
async def report_semana(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    /reportsemana <pais> [YYYY-MM-DD_inicio]
    Junta 7 'días operativos' consecutivos. Si no se indica inicio, termina en el op-day de HOY.
    """
    if len(context.args) < 1:
        return await update.message.reply_text("Uso: /reportsemana <pais> [YYYY-MM-DD_inicio]")
    country = context.args[0].lower().strip()

    if len(context.args) >= 2:
        start_str = context.args[1].strip()
        days = opday_list(SET.tz, start_str, (datetime.strptime(start_str, "%Y-%m-%d") + timedelta(days=6)).strftime("%Y-%m-%d"))
    else:
        days = list(reversed(last_n_opdays(SET.tz, 7)))  # 7 días terminando hoy

    chunks = []
    for d in days:
        s, e = opday_bounds(SET.tz, d)
        part = read_country_window(SET.data_dir, country, s, e)
        if part.strip():
            chunks.append(part)

    if not chunks:
        return await update.message.reply_text(f"Sin contenido para {country.upper()} en semana.")

    name = f"{country}-{days[0]}_a_{days[-1]}_opweek.txt"
    caption = f"{country.upper()} :: semana operativa {days[0]} → {days[-1]}"
    await _send_txt(update, name, "".join(chunks), caption)

# ---------- QUINCENA (15 días) ----------
async def report_quincena(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    /reportquincena <pais> [YYYY-MM-DD_inicio]
    Junta 15 'días operativos'. Si no se indica inicio, termina en el op-day de HOY.
    """
    if len(context.args) < 1:
        return await update.message.reply_text("Uso: /reportquincena <pais> [YYYY-MM-DD_inicio]")
    country = context.args[0].lower().strip()

    n = 15
    if len(context.args) >= 2:
        start_str = context.args[1].strip()
        days = opday_list(SET.tz, start_str, (datetime.strptime(start_str, "%Y-%m-%d") + timedelta(days=n-1)).strftime("%Y-%m-%d"))
    else:
        days = list(reversed(last_n_opdays(SET.tz, n)))

    chunks = []
    for d in days:
        s, e = opday_bounds(SET.tz, d)
        part = read_country_window(SET.data_dir, country, s, e)
        if part.strip():
            chunks.append(part)

    if not chunks:
        return await update.message.reply_text(f"Sin contenido para {country.upper()} en quincena.")

    name = f"{country}-{days[0]}_a_{days[-1]}_op15.txt"
    caption = f"{country.upper()} :: quincena operativa {days[0]} → {days[-1]}"
    await _send_txt(update, name, "".join(chunks), caption)

# ---------- MES ----------
async def report_mes(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    /reportmes <pais> <YYYY-MM>
    Junta todos los 'días operativos' cuyo inicio caiga dentro del mes indicado.
    """
    if len(context.args) < 2:
        return await update.message.reply_text("Uso: /reportmes <pais> <YYYY-MM>")
    country = context.args[0].lower().strip()
    ym = context.args[1].strip()
    try:
        first = datetime.strptime(ym + "-01", "%Y-%m-%d")
    except Exception:
        return await update.message.reply_text("Formato inválido. Usa YYYY-MM (ej. 2025-09).")

    # calcular último día del mes
    if first.month == 12:
        nextm = first.replace(year=first.year+1, month=1, day=1)
    else:
        nextm = first.replace(month=first.month+1, day=1)
    last = nextm - timedelta(days=1)

    days = opday_list(SET.tz, first.strftime("%Y-%m-%d"), last.strftime("%Y-%m-%d"))

    chunks = []
    for d in days:
        s, e = opday_bounds(SET.tz, d)
        part = read_country_window(SET.data_dir, country, s, e)
        if part.strip():
            chunks.append(part)

    if not chunks:
        return await update.message.reply_text(f"Sin contenido para {country.upper()} en {ym}.")

    name = f"{country}-{ym}_opmonth.txt"
    caption = f"{country.upper()} :: mes operativo {ym}"
    await _send_txt(update, name, "".join(chunks), caption)

# ---------- ZIP (semana/quincena/mes) con TXT individuales por op-day ----------
async def zip_period(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    /zipperiod <pais> <tipo> [YYYY-MM o YYYY-MM-DD_inicio]
    tipo ∈ {semana, quincena, mes}
    - semana/quincena: si no se indica inicio, termina hoy
    - mes: requiere YYYY-MM
    """
    if len(context.args) < 2:
        return await update.message.reply_text("Uso: /zipperiod <pais> <semana|quincena|mes> [YYYY-MM | YYYY-MM-DD_inicio]")

    country = context.args[0].lower().strip()
    kind = context.args[1].lower().strip()

    if kind not in {"semana","quincena","mes"}:
        return await update.message.reply_text("Tipo inválido. Usa: semana | quincena | mes")

    if kind == "mes":
        if len(context.args) < 3:
            return await update.message.reply_text("Para mes: /zipperiod <pais> mes <YYYY-MM>")
        ym = context.args[2].strip()
        try:
            first = datetime.strptime(ym + "-01", "%Y-%m-%d")
        except Exception:
            return await update.message.reply_text("Formato inválido. Usa YYYY-MM (ej. 2025-09).")
        if first.month == 12:
            nextm = first.replace(year=first.year+1, month=1, day=1)
        else:
            nextm = first.replace(month=first.month+1, day=1)
        last = nextm - timedelta(days=1)
        days = opday_list(SET.tz, first.strftime("%Y-%m-%d"), last.strftime("%Y-%m-%d"))
        zipname = f"{country}-{ym}_opmonth.zip"
        caption = f"{country.upper()} :: ZIP mes operativo {ym}"
    else:
        n = 7 if kind=="semana" else 15
        if len(context.args) >= 3:
            start_str = context.args[2].strip()
            days = opday_list(SET.tz, start_str, (datetime.strptime(start_str, "%Y-%m-%d")+timedelta(days=n-1)).strftime("%Y-%m-%d"))
        else:
            days = list(reversed(last_n_opdays(SET.tz, n)))
        zipname = f"{country}-{days[0]}_a_{days[-1]}_{'opweek' if n==7 else 'op15'}.zip"
        caption = f"{country.upper()} :: ZIP {kind} operativa {days[0]} → {days[-1]}"

    # Construir ZIP en memoria con un TXT por op-day
    buf = BytesIO()
    with ZipFile(buf, "w", ZIP_DEFLATED) as z:
        for d in days:
            s, e = opday_bounds(SET.tz, d)
            part = read_country_window(SET.data_dir, country, s, e)
            if part.strip():
                inner = f"{country}-{d}_opday.txt"
                z.writestr(inner, part)
    buf.seek(0)

    if buf.getbuffer().nbytes == 0:
        return await update.message.reply_text("No hay datos para empaquetar.")

    await update.message.reply_document(document=InputFile(buf, filename=zipname), caption=caption)