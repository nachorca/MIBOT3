from __future__ import annotations
import re
from urllib.parse import urlparse

import aiohttp
from telegram import Update
from telegram.ext import ContextTypes

from ..config import get_settings
from ..services.store import Store
from ..utils.time import dt_str
from ..utils.operational_day import opday_today_str
from ..utils.soup import make_soup

SET = get_settings()
STORE = Store(SET.data_dir)

# Config
MAX_BYTES = 2 * 1024 * 1024          # 2 MB por página
TIMEOUT_S = 30
UA = "Mozilla/5.0 (compatible; MIBOT3/1.0; +https://example.local)"

def _clean_text(s: str) -> str:
    s = re.sub(r"\s+", " ", s or "").strip()
    return s

def _domain(url: str) -> str:
    try:
        return urlparse(url).netloc.lower()
    except Exception:
        return "web"

async def _fetch_html(url: str) -> str:
    """Descarga la página con límite de tamaño y timeout."""
    async with aiohttp.ClientSession(headers={"User-Agent": UA}) as session:
        async with session.get(url, timeout=TIMEOUT_S) as r:
            r.raise_for_status()
            total = 0
            chunks = []
            async for chunk in r.content.iter_chunked(16384):
                total += len(chunk)
                if total > MAX_BYTES:
                    break
                chunks.append(chunk)
            return b"".join(chunks).decode(r.charset or "utf-8", errors="replace")

def _extract_headlines(html: str) -> list[str]:
    soup = make_soup(html)

    # 1) og:title y <title>
    heads: list[str] = []
    og_title = soup.find("meta", attrs={"property": "og:title"}) or soup.find("meta", attrs={"name": "og:title"})
    if og_title and og_title.get("content"):
        heads.append(_clean_text(og_title.get("content")))

    if soup.title and soup.title.string:
        heads.append(_clean_text(str(soup.title.string)))

    # 2) H1/H2 visibles
    for tag in soup.find_all(["h1", "h2"]):
        txt = _clean_text(tag.get_text(separator=" ", strip=True))
        if txt:
            heads.append(txt)

    # Normalizar, filtrar duplicados, y descartar ruido
    uniq, seen = [], set()
    for h in heads:
        key = h.lower()
        if len(h) < 6 or len(h) > 300:
            continue
        if key in seen:
            continue
        seen.add(key)
        uniq.append(h)

    return uniq[:15]  # límite razonable

# ===== NUEVO: función reutilizable por /news y por el recolector =====
async def fetch_and_store_news(country: str, url: str):
    """
    Descarga titulares de la URL y los guarda en el TXT del país (día operativo).
    Devuelve la ruta del TXT (Path) o None si no se añadieron titulares.
    """
    if not re.match(r"^https?://", url.strip()):
        return None

    try:
        html = await _fetch_html(url)
        headlines = _extract_headlines(html)
        if not headlines:
            return None

        dom = _domain(url)
        title = f"WEB {dom}"
        body = "\n".join(f"• {h}" for h in headlines)

        day = opday_today_str(SET.tz)
        fpath = STORE.append_entry(
            country=country.lower().strip(),
            day=day,
            title=title,
            dt=dt_str(SET.tz),
            text=f"{url}\n\n{body}",
        )
        return fpath
    except Exception:
        return None

# ===== Handler /news (sigue funcionando igual, pero usa la función de arriba) =====
async def news(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    /news <pais> <url>
    Descarga la página, extrae titulares y los añade al TXT del día operativo.
    """
    if len(context.args) < 2:
        return await update.message.reply_text("Uso: /news <pais> <url>\nEj: /news libia https://libyaobserver.ly/")

    country = context.args[0].lower().strip()
    url = context.args[1].strip()

    try:
        fpath = await fetch_and_store_news(country, url)
        if not fpath:
            return await update.message.reply_text("No encontré titulares útiles en la página o URL inválida.")
        await update.message.reply_text(
            f"✅ Titulares añadidos desde {_domain(url)} en {fpath.name}.\n"
            f"Ejecuta /txt {country} para ver el TXT."
        )
    except aiohttp.ClientResponseError as e:
        await update.message.reply_text(f"HTTP {e.status} al obtener la página.")
    except Exception as e:
        await update.message.reply_text(f"Error al procesar la página: {e}")
