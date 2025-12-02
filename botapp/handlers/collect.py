# botapp/handlers/collect.py
from __future__ import annotations

# --- compat: permitir ejecutar este handler como script ---
if __package__ is None or __package__ == "":
    import sys, pathlib
    sys.path.append(str(pathlib.Path(__file__).resolve().parents[2]))

from datetime import datetime, timedelta, timezone, time as dtime
import logging
import json
from pathlib import Path
from urllib.parse import urlparse
import re
from collections import defaultdict

import pytz
from telegram import Update
from telegram.ext import ContextTypes
from telethon.errors import FloodWaitError
from telethon.tl.functions.messages import ImportChatInviteRequest
from telethon.errors import (
    UserAlreadyParticipantError,
    InviteHashExpiredError,
    InviteHashInvalidError,
)
from telethon.tl.types import User as TLUser, Channel as TLChannel, Chat as TLChat
from ..services.entity_cache import EntityCache

# --- imports del paquete (compat: relativos o absolutos según ejecución) ---
try:
    from ..config import get_settings
    from ..services.telethon_client import TelethonClientHolder, TelethonConfig
    from ..services.channel_registry import ChannelRegistry
    from ..services.collect_state import CollectState
    from ..services.store import Store
    from ..utils.time import dt_str
    from ..utils.operational_day import opday_for_utc_dt
    from ..utils.meteo_header import prepend_weather_header
    # parsing y registro (DB + CSV)
    from ..services.report_hooks import registrar_incidentes_desde_lista
    from ..services.incident_parser import parse_incidents_from_text
    from ..services.web_sources import get_web_sources
    from ..services.scraper import scrape_source
except ImportError:
    from botapp.config import get_settings
    from botapp.services.telethon_client import TelethonClientHolder, TelethonConfig
    from botapp.services.channel_registry import ChannelRegistry
    from botapp.services.collect_state import CollectState
    from botapp.services.store import Store
    from botapp.utils.time import dt_str
    from botapp.utils.operational_day import opday_for_utc_dt
    from botapp.utils.meteo_header import prepend_weather_header
    # parsing y registro (DB + CSV)
    from botapp.services.report_hooks import registrar_incidentes_desde_lista
    from botapp.services.incident_parser import parse_incidents_from_text
    from botapp.services.web_sources import get_web_sources
    from botapp.services.scraper import scrape_source

log = logging.getLogger(__name__)

SET = get_settings()
REG = ChannelRegistry(SET.data_dir)
STATE = CollectState(SET.data_dir)
STORE = Store(SET.data_dir)
ECACHE = EntityCache(SET.data_dir)

# Última vez que se hizo scraping web por país (para no pegar a las webs cada pocos segundos)
WEB_LAST_SCRAPE: dict[str, datetime] = {}
WEB_MIN_INTERVAL_SECONDS = 300  # 5 minutos entre scrapings web por país

# No recolectar mensajes anteriores a esta fecha (UTC)
MIN_COLLECT_DATE = datetime(2025, 1, 1, tzinfo=timezone.utc)

# Cachés para evitar golpear el límite de resolución de usernames:
_ENTITY_CACHE: dict[str, object] = {}
_NEGATIVE_CACHE_UNTIL: dict[str, datetime] = {}

_client_holder = TelethonClientHolder(
    TelethonConfig(
        api_id=SET.telethon_api_id,
        api_hash=SET.telethon_api_hash,
        session_path=SET.telethon_session,
    )
)


async def _resolve_entity_or_join(client, ch: str):
    """
    Acepta:
      - @username
      - t.me/username -> @username
      - t.me/joinchat/XXXX  (invitación privada)
      - t.me/+XXXX          (invitación privada)
    """
    ch = (ch or "").strip()

    # Links t.me/...
    if ch.startswith("https://t.me/") or ch.startswith("http://t.me/") or ch.startswith("https://telegram.me/") or ch.startswith("http://telegram.me/"):
        path_full = urlparse(ch).path.lstrip("/")
        path = path_full.split("/", 1)[0] if path_full else ""
        # Invitaciones privadas
        if path.startswith("joinchat/") or path.startswith("+"):
            invite = path.split("/", 1)[1].strip() if path.startswith("joinchat/") else path[1:].strip()
            try:
                res = await client(ImportChatInviteRequest(invite))
            except UserAlreadyParticipantError:
                pass
            except (InviteHashExpiredError, InviteHashInvalidError) as e:
                return None, f"invite-invalida:{type(e).__name__}"
            except FloodWaitError as e:
                raise
            try:
                if hasattr(res, "chats") and res.chats:
                    return res.chats[0], None
            except Exception:
                pass
            return None, "invite-resolve-fallo"

        # Público t.me/username -> @username
        if path and not path.startswith("+") and not path.startswith("joinchat/"):
            ch = f"@{path}"

    # Cache negativa: ¿debemos esperar antes de reintentar este canal?
    now = datetime.now(timezone.utc)
    until = _NEGATIVE_CACHE_UNTIL.get(ch)
    if until and now < until:
        return None, f"defer-hasta:{until.isoformat()}"

    # Cache positiva (memoria) o persistente (input peer)
    if ch in _ENTITY_CACHE:
        return _ENTITY_CACHE[ch], None
    ip = ECACHE.get_input_peer(ch)
    if ip is not None:
        return ip, None

    # Pública @username o cualquier otro identificador
    try:
        ent = await client.get_entity(ch)
        _ENTITY_CACHE[ch] = ent
        try:
            ECACHE.remember(ch, ent)
        except Exception:
            pass
        return ent, None
    except FloodWaitError as e:
        _NEGATIVE_CACHE_UNTIL[ch] = datetime.now(timezone.utc) + timedelta(seconds=e.seconds)
        raise
    except Exception as e:
        _NEGATIVE_CACHE_UNTIL[ch] = datetime.now(timezone.utc) + timedelta(minutes=30)
        return None, f"get_entity:{type(e).__name__}:{e}"


_PAT_AT = re.compile(r"^@[A-Za-z0-9_]{5,}$")


def _is_supported_telegram_identifier(ch: str) -> bool:
    if not ch:
        return False
    s = ch.strip().strip('"')
    if _PAT_AT.match(s):
        return True
    if s.startswith("/addchannel"):
        return False
    if s.startswith("http://") or s.startswith("https://"):
        # Solo aceptamos dominios t.me / telegram.me
        return s.startswith("https://t.me/") or s.startswith("http://t.me/") or s.startswith("https://telegram.me/") or s.startswith("http://telegram.me/")
    return True


async def _set_last_id_to_latest_for_all_channels() -> None:
    """Para cada canal, fija el last_id al mensaje más reciente -> arranque limpio."""
    client = await _client_holder.get_client()
    if client is None:
        log.warning("[reset07] Telethon no disponible para reset de estado.")
        return
    for country in REG.list_countries():
        for ch in REG.list_channels(country):
            try:
                entity = await client.get_entity(ch)
                m = await client.get_messages(entity, limit=1)
                if m:
                    STATE.set_last_id(ch, m[0].id)
                    log.info(f"[reset07] {ch} last_id -> {m[0].id}")
            except Exception as e:
                log.warning(f"[reset07] fallo obteniendo último id de {ch}: {e}")


async def _ensure_today_files_with_meteo() -> None:
    """Crea el TXT del día operativo y prepende METEO si aún no existe."""
    for country in REG.list_countries():
        try:
            await prepend_weather_header(country)
        except Exception as e:
            log.warning(f"[reset07] meteo {country}: {e}")


async def reset_daily_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    """Job diario 07:00 local: arranque limpio."""
    await _set_last_id_to_latest_for_all_channels()
    await _ensure_today_files_with_meteo()
    try:
        await context.bot.send_message(
            chat_id=context.job.chat_id,
            text="✅ Reset diario: estado limpio y METEO preparada."
        )
    except Exception:
        pass


async def _collect_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Recorre todos los países/canales configurados y añade mensajes nuevos al TXT del
    DÍA OPERATIVO correcto (07:00–06:59). Además, PARSEA y REGISTRA incidentes
    por (país, op-day) -> actualiza DB y CSV automáticamente.
    """
    client = await _client_holder.get_client()
    if client is None:
        log.warning("[collect] Telethon no está configurado (api_id/api_hash/session) o no hay sesión autorizada.")
        return

    job_chat_id = getattr(getattr(context, "job", None), "chat_id", None)

    # 👇 Acumulador de texto por (country, opday) para parsear y registrar al final
    collected_by_key: dict[tuple[str, str], list[str]] = defaultdict(list)

    # ===== 1) BLOQUE TELEGRAM =====
    for country in REG.list_countries():
        chans = REG.list_channels(country)
        if not chans:
            continue

        for ch in chans:
            try:
                if not _is_supported_telegram_identifier(ch):
                    log.warning(f"[collect] '{ch}' no parece un canal de Telegram. Omite y considera moverlo a sources.json si es una web.")
                    continue

                defer_until = _NEGATIVE_CACHE_UNTIL.get(ch)
                if defer_until and datetime.now(timezone.utc) < defer_until:
                    log.info(f"[collect] diferido {ch} hasta {defer_until.isoformat()} por backoff")
                    continue

                last_id = STATE.get_last_id(ch)
                entity, reason = await _resolve_entity_or_join(client, ch)
                if entity is None:
                    log.warning(f"[collect] no se pudo resolver {ch}. ({reason})")
                    if job_chat_id is not None:
                        try:
                            await context.bot.send_message(
                                chat_id=job_chat_id,
                                text=f"❗ No se pudo resolver {ch}. {('Motivo: ' + reason) if reason else ''}"
                            )
                        except Exception:
                            pass
                    continue

                if isinstance(entity, TLUser):
                    log.warning(f"[collect] '{ch}' es un usuario, no un canal/grupo. Omite.")
                    continue

                # Traer SOLO mensajes más nuevos que last_id
                if last_id and int(last_id) > 0:
                    iter_kwargs = {"reverse": True, "limit": 200, "min_id": int(last_id)}
                    async for m in client.iter_messages(entity, **iter_kwargs):
                        if m.date and m.date < MIN_COLLECT_DATE:
                            continue
                        text = (getattr(m, "message", None) or getattr(m, "raw_text", None) or "").strip()
                        if not text:
                            continue
                        opday = opday_for_utc_dt(SET.tz, m.date)

                        # Guardar en TXT
                        STORE.append_entry(
                            country=country,
                            day=opday,
                            title=f"{ch}",
                            dt=dt_str(SET.tz),
                            text=text,
                        )
                        # Acumular para parsear/registrar después
                        collected_by_key[(country, opday)].append(text)

                        STATE.set_last_id(ch, m.id)
                        log.info(f"[collect] {ch} +{m.id} -> {opday}")
                else:
                    # Bootstrap inicial
                    batch = []
                    async for m in client.iter_messages(entity, limit=2000):  # newest -> older
                        if not getattr(m, "date", None):
                            continue
                        if m.date < MIN_COLLECT_DATE:
                            break
                        text = (getattr(m, "message", None) or getattr(m, "raw_text", None) or "").strip()
                        if not text:
                            continue
                        batch.append(m)

                    for m in reversed(batch):
                        opday = opday_for_utc_dt(SET.tz, m.date)
                        msg_text = (getattr(m, "message", None) or getattr(m, "raw_text", None) or "").strip()

                        # Guardar en TXT
                        STORE.append_entry(
                            country=country,
                            day=opday,
                            title=f"{ch}",
                            dt=dt_str(SET.tz),
                            text=msg_text,
                        )
                        # Acumular para parsear/registrar después
                        collected_by_key[(country, opday)].append(msg_text)

                        STATE.set_last_id(ch, m.id)
                        log.info(f"[collect] {ch} +{m.id} -> {opday}")

            except FloodWaitError as e:
                log.warning(f"[collect] FloodWait en {ch}: {e.seconds}s")
                if job_chat_id is not None:
                    try:
                        await context.bot.send_message(
                            chat_id=job_chat_id,
                            text=f"⚠️ FloodWait en {ch}. Reintento en {e.seconds}s."
                        )
                    except Exception:
                        pass
                return  # salir del job en este tick
            except Exception as e:
                log.exception(f"[collect] Error en {ch}: {e}")
                if job_chat_id is not None:
                    try:
                        await context.bot.send_message(
                            chat_id=job_chat_id,
                            text=f"❗ Error en {ch}: {e}"
                        )
                    except Exception:
                        pass
                # continuar con el siguiente canal

    # ===== 2) BLOQUE WEB/HTTPS (BEST-EFFORT, AISLADO) =====
    try:
        now_utc = datetime.now(timezone.utc)

        for country in REG.list_countries():
            urls = get_web_sources(country)
            if not urls:
                continue

            last = WEB_LAST_SCRAPE.get(country)
            # Solo hacer scraping web si han pasado al menos WEB_MIN_INTERVAL_SECONDS
            if last and (now_utc - last).total_seconds() < WEB_MIN_INTERVAL_SECONDS:
                continue

            WEB_LAST_SCRAPE[country] = now_utc

            for url in urls:
                url = (url or "").strip()
                if not url:
                    continue

                try:
                    # Limitar el scraping a 1 página por origen, contenido ≥ 120 chars
                    articles = await scrape_source(
                        url,
                        max_pages=1,
                        min_content_len=120,
                    )
                except Exception as e:
                    log.warning(f"[collect-web] fallo scrape {country} {url}: {e!r}")
                    continue

                if not articles:
                    continue

                opday = opday_for_utc_dt(SET.tz, now_utc)
                for art in articles:
                    title = (art.get("title") or "").strip()
                    content = (art.get("content") or "").strip()
                    if not content:
                        continue

                    text = f"{title}\n\n{content}" if title else content

                    # Guardar en TXT igual que Telegram
                    STORE.append_entry(
                        country=country,
                        day=opday,
                        title=f"WEB {url}",
                        dt=dt_str(SET.tz),
                        text=text,
                    )

                    # Acumular para parsear/registrar después junto a Telegram
                    collected_by_key[(country, opday)].append(text)

                    log.info(f"[collect-web] {country} {url} -> artículo añadido")
    except Exception as e:
        log.warning(f"[collect-web] bloque web falló: {e!r}")

    # 👉 Al final del barrido: parsear y registrar (DB + CSV) por (country, opday)
    try:
        for (country, opday), texts in collected_by_key.items():
            if not texts:
                continue
            combined = "\n".join(texts)
            # Parsear a incidentes usando tu parser heurístico
            incidents = []
            try:
                incidents = parse_incidents_from_text(combined, default_fuente="Collect")
            except Exception as e:
                log.warning(f"[collect] parser falló para {country} {opday}: {e!r}")
                continue
            if not incidents:
                continue
            # Registrar en DB y actualizar CSV del op-day
            try:
                registrar_incidentes_desde_lista(
                    pais=country.upper().capitalize(),
                    incidentes=incidents,
                    resolver_ahora=True,
                    country_hint=country.upper().capitalize(),
                    day_iso=opday,   # 🔑 clave: el CSV se escribe como incidentes_<pais>_<opday>.csv
                )
                log.info(f"[collect] registrados {len(incidents)} incidentes para {country}/{opday} (DB+CSV)")
            except Exception as e:
                log.warning(f"[collect] fallo registrando incidentes {country}/{opday}: {e!r}")
    except Exception as e:
        log.warning(f"[collect] post-proceso (parse+registro) falló: {e!r}")


async def collect_on(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """ /collect_on [interval_s] — Activa el recolector periódico (por defecto cada 60s). """
    interval = 60
    if context.args:
        try:
            interval = max(10, int(context.args[0]))  # mínimo 10 segundos
        except Exception:
            pass

    if context.job_queue is None:
        return await update.message.reply_text(
            "❌ JobQueue no disponible. Instala el extra:\n"
            "python3 -m pip install 'python-telegram-bot[job-queue]==21.6'"
        )

    name = f"collector:{update.effective_chat.id}"
    for j in context.job_queue.get_jobs_by_name(name):
        j.schedule_removal()

    context.job_queue.run_repeating(
        _collect_job,
        interval=interval,
        first=0,
        chat_id=update.effective_chat.id,
        name=name,
    )
    await update.message.reply_text(f"✅ Recolector activado cada {interval}s.")


async def collect_off(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """ /collect_off — Desactiva el recolector en este chat. """
    if context.job_queue is None:
        return await update.message.reply_text("ℹ️ No hay JobQueue inicializada.")

    name = f"collector:{update.effective_chat.id}"
    jobs = context.job_queue.get_jobs_by_name(name)
    if not jobs:
        return await update.message.reply_text("ℹ️ No había recolector activo.")
    for j in jobs:
        j.schedule_removal()
    await update.message.reply_text("🛑 Recolector desactivado.")


async def collect_now(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """ /collect_now — Ejecuta una pasada del recolector inmediatamente. """
    await _collect_job(context)
    await update.message.reply_text("✅ Recolección ejecutada una vez.")


# ---------------- HISTÓRICO ---------------- #

async def collect_fetch(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    /collect_fetch <pais> <YYYY-MM-DD> <YYYY-MM-DD>
      o: /collect_fetch <pais> from YYYY-MM-DD to YYYY-MM-DD
    Importa mensajes históricos entre las fechas (inclusive) y los guarda en los TXT del op-day.
    """
    if not context.args:
        return await update.message.reply_text(
            "Uso: /collect_fetch <pais> <YYYY-MM-DD> <YYYY-MM-DD>\n"
            "  o: /collect_fetch <pais> from YYYY-MM-DD to YYYY-MM-DD"
        )

    country = context.args[0].lower().strip()
    args = [a.strip().lower() for a in context.args[1:]]

    start_s = end_s = None
    if args and args[0] == "from" and len(args) >= 3:
        start_s = args[1]
        if args[2] == "to" and len(args) >= 4:
            end_s = args[3]
    elif len(args) >= 2:
        start_s, end_s = args[0], args[1]

    if not (start_s and end_s):
        return await update.message.reply_text("Formato inválido. Usa: /collect_fetch <pais> <YYYY-MM-DD> <YYYY-MM-DD>")

    try:
        start_dt = datetime.strptime(start_s, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        end_dt = datetime.strptime(end_s, "%Y-%m-%d").replace(tzinfo=timezone.utc) + timedelta(days=1) - timedelta(seconds=1)
        if end_dt < start_dt:
            start_dt, end_dt = end_dt, start_dt
    except Exception:
        return await update.message.reply_text("Fechas inválidas. Formato: YYYY-MM-DD")

    client = await _client_holder.get_client()
    if client is None:
        return await update.message.reply_text("Telethon no está configurado / autorizado.")

    total = 0
    per_channel = []
    for ch in REG.list_channels(country):
        count = 0
        try:
            entity = await _resolve_entity_or_join(client, ch)
            if entity is None:
                per_channel.append((ch, 0, "no-resuelto"))
                continue

            fetched = 0
            async for m in client.iter_messages(entity, limit=2000):
                md = getattr(m, "date", None)
                if not md:
                    continue
                if md < start_dt:
                    break
                if md > end_dt:
                    continue

                text = (getattr(m, "message", None) or getattr(m, "raw_text", None) or "").strip()
                if not text:
                    continue

                opday = opday_for_utc_dt(SET.tz, md)
                STORE.append_entry(
                    country=country,
                    day=opday,
                    title=f"{ch}",
                    dt=dt_str(SET.tz),
                    text=text,
                )
                count += 1
                total += 1
                fetched += 1

            per_channel.append((ch, count, "ok"))

        except FloodWaitError as fw:
            per_channel.append((ch, count, f"floodwait:{fw.seconds}s"))
            break
        except Exception as ex:
            per_channel.append((ch, count, f"error:{ex}"))

    lines = [f"✅ Importación terminada [{country}] {start_s} → {end_s}", f"Total mensajes: {total}", "Por canal:"]
    for ch, c, st in per_channel:
        lines.append(f"• {ch}: {c} ({st})")
    await update.message.reply_text("\n".join(lines))


async def collect_fetch_week(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    /collect_fetch_week <pais> [dias=7] [bloque=2]
    Importa histórico dividéndolo en bloques para evitar FloodWait.
    """
    if not context.args:
        return await update.message.reply_text("Uso: /collect_fetch_week <pais> [dias=7] [bloque=2]")

    country = context.args[0].lower().strip()
    dias = 7
    bloque = 2
    if len(context.args) >= 2:
        try:
            dias = max(1, int(context.args[1]))
        except Exception:
            dias = 7
    if len(context.args) >= 3:
        try:
            bloque = max(1, int(context.args[2]))
        except Exception:
            bloque = 2

    today = datetime.now(timezone.utc).date()
    start = today - timedelta(days=dias - 1)

    ranges = []
    cur = start
    while cur <= today:
        e = min(cur + timedelta(days=bloque - 1), today)
        ranges.append((cur, e))
        cur = e + timedelta(days=1)

    await update.message.reply_text(
        f"⏳ Importando histórico [{country}] {start.isoformat()} → {today.isoformat()} "
        f"en {len(ranges)} bloques de {bloque} día(s)…"
    )

    ok_blocks = 0
    for s, e in ranges:
        prev_args = context.args
        try:
            context.args = [country, s.strftime("%Y-%m-%d"), e.strftime("%Y-%m-%d")]
            await collect_fetch(update, context)
            ok_blocks += 1
        except Exception as ex:
            try:
                await update.message.reply_text(f"⚠️ Bloque {s}→{e} terminó con error: {ex}")
            except Exception:
                pass
        finally:
            context.args = prev_args

    await update.message.reply_text(
        f"✅ Finalizado: {ok_blocks}/{len(ranges)} bloques procesados para [{country}] "
        f"({start.isoformat()} → {today.isoformat()})."
    )


async def collect(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    /collect on [interval_s]
    /collect off
    /collect now
    /collect week <pais> [dias=7] [bloque=2]
    /collect status
    """
    if not context.args:
        return await update.message.reply_text(
            "Uso: /collect on [s] | off | now | week <pais> [dias] [bloque] | status"
        )
    sub = context.args[0].lower().strip()
    prev_args = context.args
    try:
        if sub == "on":
            context.args = prev_args[1:2]
            return await collect_on(update, context)
        if sub == "off":
            return await collect_off(update, context)
        if sub == "now":
            return await collect_now(update, context)
        if sub == "week":
            if len(prev_args) < 2:
                return await update.message.reply_text("Uso: /collect week <pais> [dias] [bloque]")
            context.args = prev_args[1:4]
            return await collect_fetch_week(update, context)
        if sub == "status":
            jq = context.job_queue
            if jq is None:
                return await update.message.reply_text("JobQueue no disponible.")
            name = f"collector:{update.effective_chat.id}"
            jobs = jq.get_jobs_by_name(name)
            if not jobs:
                return await update.message.reply_text("Recolector: inactivo")
            lines = ["Recolector: activo"]
            for j in jobs:
                nxt = getattr(j, "next_t", None) or getattr(j, "next_run_time", None)
                lines.append(f"• intervalo={getattr(j, 'interval', None) or '?'}s, next={nxt}")
            return await update.message.reply_text("\n".join(lines))
        return await update.message.reply_text(
            "Subcomando desconocido. Usa: on|off|now|week|status"
        )
    finally:
        context.args = prev_args