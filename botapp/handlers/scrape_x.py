from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, List, Optional

from telegram import Update
from telegram.ext import ContextTypes

from ..config import get_settings
from ..services.store import Store
from ..utils.time import dt_str
from ..utils.operational_day import opday_today_str
from botapp.services.x_sources import get_x_sources  # fuentes X por país (x_sources.json)

SET = get_settings()
STORE = Store(SET.data_dir)

SEEN_X = Path(SET.data_dir) / "scrape_seen_x.json"


def _load_seen() -> Dict[str, Dict[str, List[str]]]:
    if SEEN_X.exists():
        try:
            return json.loads(SEEN_X.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}


def _save_seen(d: Dict[str, Dict[str, List[str]]]) -> None:
    SEEN_X.write_text(json.dumps(d, ensure_ascii=False, indent=2), encoding="utf-8")


async def _scrape_one_handle(
    country: str,
    username: str,
    limit: int,
    message: Optional[Update] = None,
) -> int:
    """
    Scrapea hasta `limit` tweets de @username (desde X/Twitter) y los añade al STORE
    si no estaban ya en scrape_seen_x.json.

    Devuelve el número de tweets añadidos.
    Si `message` no es None, envía avisos de error al chat; si es None, imprime por consola.
    """
    username = username.lstrip("@").strip()
    if not username:
        return 0

    # 1) Importar snscrape (ya estamos en Python 3.11, no necesitamos parches)
    try:
        import snscrape.modules.twitter as sntwitter
    except Exception as e:
        txt = (
            "No se pudo importar snscrape.\n"
            "- Asegúrate de que está instalado en este entorno (.venv311).\n"
            "  Ejemplo: python -m pip install snscrape\n"
            f"Error real: {e}"
        )
        if message:
            await message.reply_text(txt)
        else:
            print(f"[scrape_x_job] {txt}")
        return 0

    # 2) Cargar / inicializar SEEN y día operativo
    seen: Dict[str, Dict[str, List[str]]] = _load_seen()
    seen.setdefault(country, {})
    seen[country].setdefault(username, [])

    day = opday_today_str(SET.tz)

    # 3) Recoger tweets con snscrape
    try:
        scraper = sntwitter.TwitterUserScraper(username)
        tweets = []
        for i, t in enumerate(scraper.get_items()):
            if i >= limit:
                break
            tweets.append({
                "id": getattr(t, "id", None),
                "date": getattr(t, "date", None),
                "content": getattr(t, "rawContent", "") or getattr(t, "content", ""),
                "url": f"https://x.com/{username}/status/{getattr(t, 'id', '')}",
            })
    except Exception as e:
        txt = f"No puedo obtener tweets de @{username}: {e}"
        if message:
            await message.reply_text(txt)
        else:
            print(f"[scrape_x_job] {txt}")
        return 0

    if not tweets:
        txt = f"No hubo tweets recientes para @{username}."
        if message:
            await message.reply_text(txt)
        else:
            print(f"[scrape_x_job] {txt}")
        return 0

    # 4) Añadir solo tweets nuevos
    added = 0
    for tw in tweets:
        tid = str(tw.get("id") or "")
        if not tid or tid in seen[country][username]:
            continue

        text = (tw.get("content") or "").strip()
        if not text:
            continue

        url = (tw.get("url") or "").strip()
        dtline = str(tw.get("date") or "")

        STORE.append_entry(
            country=country,
            day=day,
            title=f"X @{username}",
            dt=dt_str(SET.tz),
            text=f"{dtline}\n{url}\n\n{text}",
        )
        seen[country][username].append(tid)
        added += 1

    _save_seen(seen)
    return added


# ======================================================
#          COMANDO MANUAL: /scrape_x
# ======================================================
async def scrape_x(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    /scrape_x <pais> <usuario|all> [limit=10]

    Ejemplos:
      /scrape_x libia libyaobserver 15
      /scrape_x haiti HaitiLibre 20
      /scrape_x haiti all 30   -> recorre todas las fuentes X de haiti en x_sources.json
    """
    message = update.message or update.effective_message

    args = context.args or []
    if len(args) < 2:
        return await message.reply_text("Uso: /scrape_x <pais> <usuario|all> [limit=10]")

    country = args[0].lower().strip()
    user_arg = args[1].strip()
    limit = 10

    if len(args) >= 3:
        try:
            limit = max(1, int(args[2]))
        except Exception:
            limit = 10

    user_arg = user_arg.lstrip("@")

    # Caso: ALL → usar las fuentes de x_sources.json
    if user_arg.lower() == "all":
        handles = get_x_sources(country)
        if not handles:
            return await message.reply_text(
                f"No hay fuentes X configuradas para el país: {country}"
            )

        await message.reply_text(
            f"⏳ Iniciando scraping X para {len(handles)} cuentas de {country} (límite={limit})…"
        )

        total_added = 0
        total_handles = 0
        for h in handles:
            handle_clean = h.lstrip("@")
            if not handle_clean:
                continue
            total_handles += 1
            added = await _scrape_one_handle(country, handle_clean, limit, message)
            total_added += added

        return await message.reply_text(
            f"✅ Scraping X completado para {total_handles} cuentas de {country}. "
            f"Tweets añadidos: {total_added}."
        )

    # Caso: un único usuario
    username = user_arg
    await message.reply_text(
        f"⏳ Scrapeando X para {country}/@{username} (límite={limit})…"
    )

    added = await _scrape_one_handle(country, username, limit, message)
    return await message.reply_text(
        f"✅ Scraping X completado. Añadidos {added} tweets de @{username} en {country}."
    )


# ======================================================
#          JOB: /scrape_x_job  (cada 10 minutos)
# ======================================================
async def scrape_x_job_callback(context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Callback del JobQueue. Se ejecuta automáticamente cada N segundos
    con los datos configurados por /scrape_x_job.
    """
    job = context.job
    data = job.data or {}
    country = str(data.get("country", "haiti")).lower()
    limit = int(data.get("limit", 10))
    chat_id = job.chat_id

    handles = get_x_sources(country)
    if not handles:
        await context.bot.send_message(
            chat_id=chat_id,
            text=f"[scrape_x_job] No hay fuentes X configuradas para {country}.",
        )
        return

    total_added = 0
    total_handles = 0
    for h in handles:
        handle_clean = h.lstrip("@")
        if not handle_clean:
            continue
        total_handles += 1
        added = await _scrape_one_handle(country, handle_clean, limit, None)
        total_added += added

    await context.bot.send_message(
        chat_id=chat_id,
        text=(
            f"⏱️ Job /scrape_x_job ({country}) ejecutado.\n"
            f"Cuentas: {total_handles} | Tweets nuevos añadidos: {total_added}."
        ),
    )


async def scrape_x_job(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    /scrape_x_job <pais> [limit]

    Programa un job que cada 10 minutos:
      - Lee las fuentes X de ese país (x_sources.json)
      - Scrapea cada cuenta
      - Añade solo tweets nuevos al STORE
      - Envía un resumen al chat
    """
    message = update.message or update.effective_message
    args = context.args or []

    if len(args) < 1:
        return await message.reply_text(
            "Uso: /scrape_x_job <pais> [limit=10]\n"
            "Ejemplo: /scrape_x_job haiti 20"
        )

    country = args[0].lower().strip()
    limit = 10
    if len(args) >= 2:
        try:
            limit = max(1, int(args[1]))
        except Exception:
            limit = 10

    chat_id = message.chat_id

    # Eliminar jobs anteriores para ese país (si los hubiera)
    job_name = f"scrape_x_job_{country}"
    for job in context.job_queue.get_jobs_by_name(job_name):
        job.schedule_removal()

    # Programar job cada 600 segundos (10 minutos)
    context.job_queue.run_repeating(
        scrape_x_job_callback,
        interval=600,
        first=0,
        chat_id=chat_id,
        name=job_name,
        data={"country": country, "limit": limit},
    )

    await message.reply_text(
        f"✅ Job /scrape_x_job configurado para {country} cada 10 minutos "
        f"(limit={limit})."
    )