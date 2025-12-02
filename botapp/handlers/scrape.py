from __future__ import annotations
import asyncio
import json
import logging
import os
import time
from pathlib import Path
from typing import Callable, Optional, Sequence

from telegram import Update
from telegram.ext import ContextTypes

from ..config import get_settings
from ..services.store import Store
from ..services.scraper import scrape_source
from ..utils.time import dt_str
from ..utils.operational_day import opday_today_str

SET = get_settings()
STORE = Store(SET.data_dir)
log = logging.getLogger(__name__)

_SCRAPE_LOCK: asyncio.Lock | None = None

_FULL_TOKENS = {"full", "todo", "todos", "all", "global", "*", "∞", "inf", "infinite", "completo"}


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return int(raw.strip())
    except Exception:
        return default


_DEFAULT_MAX_PAGES_RAW = _env_int("SCRAPE_MAX_PAGES", 5)
SCRAPE_DEFAULT_MAX_PAGES: Optional[int] = _DEFAULT_MAX_PAGES_RAW if _DEFAULT_MAX_PAGES_RAW > 0 else None
SCRAPE_DEFAULT_MIN_LEN = max(10, _env_int("SCRAPE_MIN_LEN", 50))
_DEFAULT_VISIT_FACTOR_RAW = _env_int("SCRAPE_VISIT_FACTOR", 3)
SCRAPE_DEFAULT_VISIT_FACTOR: Optional[int] = (
    _DEFAULT_VISIT_FACTOR_RAW if _DEFAULT_VISIT_FACTOR_RAW > 0 else None
)
_DEFAULT_MAX_VISITS_RAW = _env_int("SCRAPE_MAX_VISITS", 0)
SCRAPE_DEFAULT_MAX_VISITS: Optional[int] = (
    _DEFAULT_MAX_VISITS_RAW if _DEFAULT_MAX_VISITS_RAW > 0 else None
)

# Límites adicionales de volumen (se pueden ajustar por variables de entorno)
SCRAPE_MAX_ITEMS_PER_COUNTRY = _env_int("SCRAPE_MAX_ITEMS_PER_COUNTRY", 200)
SCRAPE_MAX_ITEMS_PER_DOMAIN = _env_int("SCRAPE_MAX_ITEMS_PER_DOMAIN", 50)


def _parse_page_limit(value: Optional[str], default: Optional[int]) -> Optional[int]:
    if value is None:
        return default
    token = value.strip().lower()
    if not token:
        return default
    if token in _FULL_TOKENS:
        return None
    try:
        parsed = int(token)
    except Exception:
        return default
    return parsed if parsed > 0 else None


def _parse_min_len(value: Optional[str], default: int) -> int:
    if value is None:
        return default
    try:
        parsed = int(value)
    except Exception:
        return default
    return max(10, parsed)


def _parse_visit_factor(value: Optional[str], default: Optional[int]) -> Optional[int]:
    if value is None:
        return default
    token = value.strip()
    if not token:
        return default
    try:
        parsed = int(token)
    except Exception:
        return default
    return parsed if parsed > 0 else None


def _format_limit(value: Optional[int]) -> str:
    return "∞" if value is None else str(value)


def _parse_page_limit_any(value: Optional[object], default: Optional[int]) -> Optional[int]:
    if isinstance(value, str) or value is None:
        return _parse_page_limit(value, default)
    try:
        parsed = int(value)  # type: ignore[arg-type]
    except Exception:
        return default
    return parsed if parsed > 0 else None


def _parse_min_len_any(value: Optional[object], default: int) -> int:
    if isinstance(value, str) or value is None:
        return _parse_min_len(value, default)
    try:
        parsed = int(value)  # type: ignore[arg-type]
    except Exception:
        return default
    return max(10, parsed)


def _parse_visit_factor_any(value: Optional[object], default: Optional[int]) -> Optional[int]:
    if isinstance(value, str) or value is None:
        return _parse_visit_factor(value, default)
    try:
        parsed = int(value)  # type: ignore[arg-type]
    except Exception:
        return default
    return parsed if parsed > 0 else None


def _get_scrape_lock() -> asyncio.Lock:
    """
    Devuelve un candado global para serializar ejecuciones del scraper.
    Se inicializa bajo demanda para evitar problemas con event loops.
    """
    global _SCRAPE_LOCK
    if _SCRAPE_LOCK is None:
        _SCRAPE_LOCK = asyncio.Lock()
    return _SCRAPE_LOCK


# dedupe file
SEEN_FILE = Path(SET.data_dir) / "scrape_seen.json"
SourcesDict = dict[str, Sequence[str]]


def _load_seen() -> dict:
    if SEEN_FILE.exists():
        try:
            return json.loads(SEEN_FILE.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}


def _save_seen(data: dict) -> None:
    SEEN_FILE.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def _dom(url: str) -> str:
    try:
        return url.split("/")[2]
    except Exception:
        return "web"


# Filtro simple de palabras clave para quedarnos solo con contenido relevante SICU
_KEYWORDS = [
    # English
    "attack",
    "killed",
    "deaths",
    "shooting",
    "gunfire",
    "explosion",
    "blast",
    "airstrike",
    "air strike",
    "shelling",
    "clash",
    "armed group",
    "militia",
    "kidnap",
    "abduction",
    "hostage",
    "riot",
    "protest",
    "demonstration",
    "looting",
    "robbery",
    "gang",
    # Español / FR / PT (raíces aproximadas)
    "ataque",
    "muert",
    "herid",
    "tiroteo",
    "disparo",
    "explos",
    "bomba",
    "enfrentamiento",
    "enfrent",
    "choque",
    "secuest",
    "homicid",
    "asesinat",
    "disturb",
    "protesta",
    "manifestac",
    "bloqueo",
    "barricada",
    "saqueo",
]


def _has_relevant_keywords(title: str, content: str) -> bool:
    text = f"{title} {content}".lower()
    return any(k in text for k in _KEYWORDS)


def _load_sources_config() -> SourcesDict:
    """
    Carga las fuentes web desde data/web_sources.json

    Estructura esperada:
        {
            "haiti": ["https://...", "https://..."],
            "libia": [...],
            ...
        }
    """
    sources_file = Path(SET.data_dir) / "web_sources.json"
    if not sources_file.exists():
        raise FileNotFoundError("No existe data/web_sources.json")
    try:
        data = json.loads(sources_file.read_text(encoding="utf-8"))
    except Exception as exc:
        raise ValueError(f"Error leyendo web_sources.json: {exc}") from exc
    if not isinstance(data, dict):
        raise ValueError("El archivo web_sources.json debe ser un diccionario pais -> [urls].")
    return data


def _get_summarizer() -> tuple[bool, Optional[Callable[[str, str], str]]]:
    try:
        from ..services.ai import summarize_article_es  # opcional

        return True, summarize_article_es
    except Exception:
        return False, None


async def _scrape_country_sources(
    country: str,
    urls: Sequence[str],
    *,
    max_pages: Optional[int],
    min_len: int,
    day: str,
    seen: dict,
    use_ai: bool,
    summarize: Optional[Callable[[str, str], str]],
    visit_factor: Optional[int],
    max_visits: Optional[int],
) -> tuple[int, list[tuple[str, int, str]]]:
    """
    Raspa todas las URLs de un país, aplicando:
    - dedupe por URL (scrape_seen.json)
    - límite de ítems por país y por dominio
    - filtro HTTPS
    - filtro de longitud y palabras clave relevantes
    """
    seen_list = seen.setdefault(country, [])
    seen_set = set(seen_list)
    total = 0
    per_site: list[tuple[str, int, str]] = []
    per_domain_count: dict[str, int] = {}

    for url in urls:
        # Si ya hemos llegado al máximo por país, no seguimos con más fuentes.
        if SCRAPE_MAX_ITEMS_PER_COUNTRY > 0 and total >= SCRAPE_MAX_ITEMS_PER_COUNTRY:
            per_site.append((url, 0, "skip:max_country"))
            continue

        try:
            arts = await scrape_source(
                url,
                max_pages=max_pages,
                min_content_len=min_len,
                visit_factor=visit_factor,
                max_visits=max_visits,
            )
            count = 0
            for a in arts:
                link = (a.get("url") or "").strip()
                title = (a.get("title") or "").strip()
                content = (a.get("content") or "").strip()

                # Requisitos mínimos estrictos
                if not (link and title and content):
                    continue
                if not link.startswith("https://"):
                    # Forzamos solo HTTPS
                    continue
                if len(content) < min_len:
                    continue
                if not _has_relevant_keywords(title, content):
                    # Filtro SICU básico por palabras clave
                    continue
                if link in seen_set:
                    # Ya lo guardamos en ejecuciones anteriores
                    continue

                # Límite por dominio
                dom = _dom(link)
                dom_count = per_domain_count.get(dom, 0)
                if SCRAPE_MAX_ITEMS_PER_DOMAIN > 0 and dom_count >= SCRAPE_MAX_ITEMS_PER_DOMAIN:
                    continue

                summary = ""
                if use_ai and summarize:
                    try:
                        summary = summarize(title, content)
                    except Exception:
                        summary = ""

                body_lines = [title, link, ""]
                if summary:
                    body_lines.append("Resumen:")
                    body_lines.append(summary)
                    body_lines.append("")
                body_lines.append(content[:2000])

                STORE.append_entry(
                    country=country,
                    day=day,
                    title=f"WEB {_dom(link)}",
                    dt=dt_str(SET.tz),
                    text="\n".join(body_lines),
                )
                seen_list.append(link)
                seen_set.add(link)

                per_domain_count[dom] = dom_count + 1
                count += 1
                total += 1

                # Cortes duros por país / dominio para no saturar
                if SCRAPE_MAX_ITEMS_PER_COUNTRY > 0 and total >= SCRAPE_MAX_ITEMS_PER_COUNTRY:
                    break
                if SCRAPE_MAX_ITEMS_PER_DOMAIN > 0 and per_domain_count[dom] >= SCRAPE_MAX_ITEMS_PER_DOMAIN:
                    # No hace falta seguir con más artículos de este dominio
                    continue

            per_site.append((url, count, "ok"))
        except Exception as ex:
            per_site.append((url, 0, f"error:{ex}"))
    return total, per_site


async def _scrape_all_core(
    sources: SourcesDict,
    *,
    max_pages: Optional[int],
    min_len: int,
    seen: dict,
    use_ai: bool,
    summarize: Optional[Callable[[str, str], str]],
    visit_factor: Optional[int],
    max_visits: Optional[int],
) -> tuple[int, list[tuple[str, int, list[tuple[str, int, str]]]]]:
    overall_total = 0
    country_reports: list[tuple[str, int, list[tuple[str, int, str]]]] = []
    day = opday_today_str(SET.tz)
    for country in sorted(sources.keys()):
        urls = sources.get(country, []) or []
        if not urls:
            country_reports.append((country, 0, []))
            continue
        total, per_site = await _scrape_country_sources(
            country,
            urls,
            max_pages=max_pages,
            min_len=min_len,
            day=day,
            seen=seen,
            use_ai=use_ai,
            summarize=summarize,
            visit_factor=visit_factor,
            max_visits=max_visits,
        )
        overall_total += total
        country_reports.append((country, total, per_site))
    return overall_total, country_reports


def _build_scrape_summary(
    *,
    countries: int,
    max_pages: Optional[int],
    min_len: int,
    visit_factor: Optional[int],
    overall_total: int,
    reports: Sequence[tuple[str, int, Sequence[tuple[str, int, str]]]],
) -> str:
    lines = [
        (
            f"✅ Scraping global completado. Países: {countries}  "
            f"max_paginas={_format_limit(max_pages)}  min_len={min_len}  "
            f"visit_factor={_format_limit(visit_factor)}"
        ),
        f"Total artículos añadidos: {overall_total}",
        f"Límite por país: {SCRAPE_MAX_ITEMS_PER_COUNTRY}  |  límite por dominio: {SCRAPE_MAX_ITEMS_PER_DOMAIN}",
        "Por país:",
    ]
    for country, tot, per_site in reports:
        lines.append(f"• {country}: {tot}")
        for url, count, status in per_site:
            lines.append(f"    - {url}: {count} ({status})")
    return "\n".join(lines)


async def scrape(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    /scrape <pais> [max_paginas|full] [min_len=50] [visit_factor=3]
    Raspa las webs de data/web_sources.json para ese país, aplica dedupe por URL y
    añade al TXT del día operativo. Si OPENAI_API_KEY está presente, añade un breve resumen.
    """
    # Si no se pasan args, ejecutar scraping global (comodidad):
    if not context.args:
        # Reusar la implementación global
        return await scrape_all(update, context)

    country = context.args[0].lower().strip()
    # Atajo: permitir `/scrape all` (o 'global'/'todos') como alias para scraping global
    if country in ("all", "global", "todos", "todo", "allcountries"):
        # pasar los args restantes a scrape_all (p.ej. `/scrape all 3 50`)
        prev_args = context.args
        try:
            context.args = context.args[1:]
            return await scrape_all(update, context)
        finally:
            context.args = prev_args
    max_pages_arg = context.args[1] if len(context.args) >= 2 else None
    min_len_arg = context.args[2] if len(context.args) >= 3 else None
    visit_factor_arg = context.args[3] if len(context.args) >= 4 else None
    max_pages = _parse_page_limit(max_pages_arg, SCRAPE_DEFAULT_MAX_PAGES)
    min_len = _parse_min_len(min_len_arg, SCRAPE_DEFAULT_MIN_LEN)
    visit_factor = _parse_visit_factor(visit_factor_arg, SCRAPE_DEFAULT_VISIT_FACTOR)
    max_visits = SCRAPE_DEFAULT_MAX_VISITS

    try:
        sources = _load_sources_config()
    except FileNotFoundError:
        return await update.message.reply_text("No existe data/web_sources.json")
    except ValueError as exc:
        return await update.message.reply_text(str(exc))

    urls = sources.get(country, [])
    if not urls:
        return await update.message.reply_text(f"No hay fuentes configuradas para {country} en web_sources.json")

    # Dedupe (por país → set de URLs)
    use_ai, summarize = _get_summarizer()
    lock = _get_scrape_lock()
    if lock.locked() and update.message:
        await update.message.reply_text(
            "⌛ Otro scraping está en curso, se esperará a que termine antes de continuar…"
        )

    total = 0
    per_site: list[tuple[str, int, str]] = []
    async with lock:
        seen = _load_seen()
        seen.setdefault(country, [])
        day = opday_today_str(SET.tz)
        total, per_site = await _scrape_country_sources(
            country,
            urls,
            max_pages=max_pages,
            min_len=min_len,
            day=day,
            seen=seen,
            use_ai=use_ai,
            summarize=summarize,
            visit_factor=visit_factor,
            max_visits=max_visits,
        )
        _save_seen(seen)
    lines = [
        (
            f"✅ Scraping completado [{country}] fuentes={len(urls)} "
            f"max_paginas={_format_limit(max_pages)} min_len={min_len} "
            f"visit_factor={_format_limit(visit_factor)}"
        ),
        f"Límite por país: {SCRAPE_MAX_ITEMS_PER_COUNTRY}  |  límite por dominio: {SCRAPE_MAX_ITEMS_PER_DOMAIN}",
        f"Total artículos añadidos: {total}",
        "Por fuente:",
    ]
    for u, c, st in per_site:
        lines.append(f"• {u}: {c} ({st})")
    await update.message.reply_text("\n".join(lines))


async def scrape_all(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    /scrape_all [max_paginas|full] [min_len=50] [visit_factor=3]
    Raspa todas las fuentes definidas en data/web_sources.json para todos los países.
    Mantiene la dedupe por país (data/scrape_seen.json), el guardado en TXT y el resumen opcional con AI.
    """
    # Parsear args (opcionalmente max_pages, min_len y visit_factor)
    args = context.args or []
    max_pages_arg = args[0] if args else None
    min_len_arg = args[1] if len(args) >= 2 else None
    visit_factor_arg = args[2] if len(args) >= 3 else None
    max_pages = _parse_page_limit(max_pages_arg, SCRAPE_DEFAULT_MAX_PAGES)
    min_len = _parse_min_len(min_len_arg, SCRAPE_DEFAULT_MIN_LEN)
    visit_factor = _parse_visit_factor(visit_factor_arg, SCRAPE_DEFAULT_VISIT_FACTOR)
    max_visits = SCRAPE_DEFAULT_MAX_VISITS

    try:
        sources = _load_sources_config()
    except FileNotFoundError:
        return await update.message.reply_text("No existe data/web_sources.json")
    except ValueError as exc:
        return await update.message.reply_text(str(exc))

    use_ai, summarize = _get_summarizer()
    lock = _get_scrape_lock()
    if lock.locked() and update.message:
        await update.message.reply_text(
            "⌛ Esperando a que finalice otro scraping antes de iniciar este…"
        )

    seen = {}
    overall_total = 0
    country_reports: list[tuple[str, int, list[tuple[str, int, str]]]] = []
    async with lock:
        seen = _load_seen()
        overall_total, country_reports = await _scrape_all_core(
            sources,
            max_pages=max_pages,
            min_len=min_len,
            seen=seen,
            use_ai=use_ai,
            summarize=summarize,
            visit_factor=visit_factor,
            max_visits=max_visits,
        )
        _save_seen(seen)
    summary = _build_scrape_summary(
        countries=len(sources),
        max_pages=max_pages,
        min_len=min_len,
        visit_factor=visit_factor,
        overall_total=overall_total,
        reports=country_reports,
    )
    await update.message.reply_text(summary)


async def scrape_auto_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Job programado: ejecuta scrape_all cada X minutos.
    Usa job.data para permitir overrides (max_pages/min_len/visit_factor/max_visits) si fuese necesario.
    """
    job = getattr(context, "job", None)
    data = getattr(job, "data", {}) or {}
    max_pages = _parse_page_limit_any(data.get("max_pages"), SCRAPE_DEFAULT_MAX_PAGES)
    min_len = _parse_min_len_any(data.get("min_len"), SCRAPE_DEFAULT_MIN_LEN)
    visit_factor = _parse_visit_factor_any(data.get("visit_factor"), SCRAPE_DEFAULT_VISIT_FACTOR)
    max_visits = _parse_visit_factor_any(data.get("max_visits"), SCRAPE_DEFAULT_MAX_VISITS)
    chat_id = getattr(job, "chat_id", None)

    lock = _get_scrape_lock()
    if lock.locked():
        log.info("[scrape-auto] Otra ejecución sigue en curso; se esperará a que libere el candado.")

    start = time.perf_counter()
    summary: Optional[str] = None

    async with lock:
        try:
            sources = _load_sources_config()
        except FileNotFoundError as exc:
            log.warning("[scrape-auto] %s", exc)
            if chat_id:
                try:
                    await context.bot.send_message(chat_id=chat_id, text=str(exc))
                except Exception:
                    log.exception("[scrape-auto] No se pudo notificar chat_id=%s", chat_id)
            return
        except ValueError as exc:
            log.warning("[scrape-auto] %s", exc)
            if chat_id:
                try:
                    await context.bot.send_message(chat_id=chat_id, text=str(exc))
                except Exception:
                    log.exception("[scrape-auto] No se pudo notificar chat_id=%s", chat_id)
            return

        seen = _load_seen()
        use_ai, summarize = _get_summarizer()
        try:
            overall_total, country_reports = await _scrape_all_core(
                sources,
                max_pages=max_pages,
                min_len=min_len,
                seen=seen,
                use_ai=use_ai,
                summarize=summarize,
                visit_factor=visit_factor,
                max_visits=max_visits,
            )
        except Exception as exc:
            log.exception("[scrape-auto] Error durante scraping periódico: %s", exc)
            if chat_id:
                try:
                    await context.bot.send_message(
                        chat_id=chat_id,
                        text=f"❗ Error en scraping automático: {exc}",
                    )
                except Exception:
                    log.exception("[scrape-auto] No se pudo notificar chat_id=%s", chat_id)
            return

        _save_seen(seen)
        summary = _build_scrape_summary(
            countries=len(sources),
            max_pages=max_pages,
            min_len=min_len,
            visit_factor=visit_factor,
            overall_total=overall_total,
            reports=country_reports,
        )
        first_line = summary.splitlines()[0] if summary else "Scraping completado."
        duration = time.perf_counter() - start
        log.info("[scrape-auto] Finalizado en %.1fs. %s", duration, first_line)

    if summary and chat_id:
        try:
            await context.bot.send_message(chat_id=chat_id, text=summary)
        except Exception:
            log.exception("[scrape-auto] No se pudo notificar chat_id=%s", chat_id)