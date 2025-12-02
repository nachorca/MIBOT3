from __future__ import annotations

import asyncio
import json
import re
from collections import deque
import os
from typing import Callable, Dict, List, Optional, Set, Tuple
from urllib.parse import urljoin, urlparse

import aiohttp
from aiohttp import ClientTimeout

from ..utils.soup import make_soup

USER_AGENT = "Mozilla/5.0 (compatible; MIBOT3/1.0; +https://example.local)"
MAX_BYTES = 2 * 1024 * 1024  # 2 MB por respuesta
DEFAULT_CONCURRENCY = 5
DEFAULT_TIMEOUT = ClientTimeout(total=12, sock_connect=5, sock_read=10)

_VISIT_FACTOR_ENV = int(os.getenv("SCRAPE_VISIT_FACTOR", "3") or 0)
SCRAPE_VISIT_FACTOR: Optional[int] = _VISIT_FACTOR_ENV if _VISIT_FACTOR_ENV > 0 else None
_MAX_VISITS_ENV = int(os.getenv("SCRAPE_MAX_VISITS", "0") or 0)
SCRAPE_MAX_VISITS: Optional[int] = _MAX_VISITS_ENV if _MAX_VISITS_ENV > 0 else None
_QUEUE_LIMIT_ENV = int(os.getenv("SCRAPE_QUEUE_LIMIT", "5000") or 0)
QUEUE_LIMIT_FALLBACK = _QUEUE_LIMIT_ENV if _QUEUE_LIMIT_ENV > 0 else 5000

# Patrones de ruido que queremos eliminar del contenido extraído (footers, banners, etc.)
# Añadimos específicamente el mensaje de instalación PWA en iOS/iPad observado en algunas webs.
NOISE_PATTERNS: List[re.Pattern[str]] = [
    re.compile(r"install\s+pwa\s+using\s+add\s+to\s+home\s+screen", re.IGNORECASE),
    re.compile(r"for\s+ios\s+and\s+ipad\s+browsers.*add\s+to\s+(home\s+screen|dock)", re.IGNORECASE),
    re.compile(r"add\s+to\s+home\s+screen\s+in\s+ios\s+safari", re.IGNORECASE),
]


def _extract_reuters(html: str) -> Tuple[str, str]:
    """
    Reuters entrega el cuerpo en JSON-LD; lo parseamos para obtener headline y articleBody.
    """
    soup = make_soup(html)
    for script in soup.find_all("script", attrs={"type": "application/ld+json"}):
        text = script.string or script.get_text(strip=True)
        if not text:
            continue
        try:
            data = json.loads(text)
        except Exception:
            continue
        nodes = data if isinstance(data, list) else [data]
        for node in nodes:
            if not isinstance(node, dict):
                continue
            types = node.get("@type") or node.get("type") or ""
            if isinstance(types, list):
                types_low = [str(t).lower() for t in types]
            else:
                types_low = [str(types).lower()]
            if not any(t in ("newsarticle", "article") for t in types_low):
                continue
            body = node.get("articleBody") or node.get("description")
            if not body:
                continue
            if isinstance(body, list):
                body = "\n".join(str(x) for x in body)
            headline = node.get("headline") or node.get("name") or ""
            return headline.strip(), str(body).strip()
    return "", ""


def _extract_unrwa(html: str) -> Tuple[str, str]:
    """
    Las notas de prensa de UNRWA están dentro de divs con clases node--type-news-story.
    """
    soup = make_soup(html)
    article = soup.find("div", class_=lambda c: c and "node--type-news-story" in c)
    if not article:
        article = soup.find("article")
    if not article:
        return "", ""
    title_tag = article.find(["h1", "h2"])
    title = title_tag.get_text(strip=True) if title_tag else ""
    parts: List[str] = []
    for tag in article.find_all(["p", "li"]):
        txt = tag.get_text(" ", strip=True)
        if txt:
            parts.append(txt)
    content = "\n".join(parts).strip()
    return title, content


SPECIAL_EXTRACTORS: Dict[str, Callable[[str], Tuple[str, str]]] = {
    "reuters.com": _extract_reuters,
    "unrwa.org": _extract_unrwa,
}


def _strip_noise(text: str) -> str:
    """
    Elimina líneas que coincidan con patrones de ruido definidos en NOISE_PATTERNS.
    Conserva saltos de línea razonables.
    """
    if not text:
        return text
    lines = [ln for ln in text.splitlines() if ln.strip()]
    kept: List[str] = []
    for ln in lines:
        lns = ln.strip()
        if any(p.search(lns) for p in NOISE_PATTERNS):
            continue
        # Normaliza espacios dentro de la línea pero preserva saltos de línea
        lns = re.sub(r"\s+", " ", lns)
        kept.append(lns)
    cleaned = "\n".join(kept)
    # Reduce bloques muy largos de saltos de línea a dobles saltos
    cleaned = re.sub(r"\n{3,}", "\n\n", cleaned)
    return cleaned.strip()


def _normalize_url(base: str, href: str) -> str | None:
    if not href:
        return None
    href = href.strip()
    if href.startswith("#") or href.startswith("mailto:") or href.startswith("javascript:"):
        return None
    try:
        u = urljoin(base, href)
        parsed = urlparse(u)
        return parsed._replace(fragment="").geturl()
    except Exception:
        return None


def _same_domain(a: str, b: str) -> bool:
    try:
        da = urlparse(a).netloc.lower()
        db = urlparse(b).netloc.lower()
        return da == db
    except Exception:
        return False


async def _fetch_text(session: aiohttp.ClientSession, url: str) -> str:
    async with session.get(url) as r:
        r.raise_for_status()
        total = 0
        parts: List[bytes] = []
        async for chunk in r.content.iter_chunked(16384):
            total += len(chunk)
            if total > MAX_BYTES:
                break
            parts.append(chunk)
        enc = r.charset or "utf-8"
        return b"".join(parts).decode(enc, errors="replace")


def _extract_article(html: str, url: str) -> Tuple[str, str, Optional[object]]:
    """
    Extrae (title, content) básico de una página HTML.
    """
    domain = urlparse(url).netloc.lower()
    extractor = SPECIAL_EXTRACTORS.get(domain) or SPECIAL_EXTRACTORS.get(domain.removeprefix("www."))
    if extractor:
        try:
            title, content = extractor(html)
            if title or content:
                title = _strip_noise(re.sub(r"\s+", " ", title).strip())
                content = _strip_noise(re.sub(r"\n{3,}", "\n\n", content).strip())
                if title or content:
                    return title, content, None
        except Exception:
            pass

    soup = make_soup(html)
    title = ""
    og = soup.find("meta", attrs={"property": "og:title"}) or soup.find("meta", attrs={"name": "og:title"})
    if og and og.get("content"):
        title = og["content"].strip()
    if not title and soup.title and soup.title.string:
        title = str(soup.title.string).strip()

    texts: List[str] = []
    for tag in soup.find_all(["h1", "h2", "p"]):
        txt = tag.get_text(" ", strip=True)
        if txt and len(txt) > 40:
            texts.append(txt)
    content = "\n".join(texts)
    # Limpieza de títulos y contenido
    title = _strip_noise(re.sub(r"\s+", " ", title).strip())
    content = _strip_noise(re.sub(r"\n{3,}", "\n\n", content).strip())
    return title, content, soup


async def _collect_web(context):
    """
    Placeholder collector; implementaremos aquí la integración con get_web_sources()
    y el almacenamiento en TXT/STORE cuando conectemos el scraping web
    al ciclo de /collect.
    """
    return


async def scrape_source(
    url: str,
    max_pages: Optional[int] = 5,
    min_content_len: int = 100,
    *,
    concurrency: int = DEFAULT_CONCURRENCY,
    visit_factor: Optional[int] = None,
    max_visits: Optional[int] = None,
) -> List[Dict[str, str]]:
    """
    Recorre hasta max_pages páginas dentro del mismo dominio a partir de 'url'.
    Devuelve artículos [{url,title,content}].
    """
    seen: Set[str] = set()
    queue: deque[str] = deque([url])
    results: List[Dict[str, str]] = []
    visits = 0
    target_pages: Optional[int] = max_pages if max_pages and max_pages > 0 else None
    effective_visit_factor = visit_factor if visit_factor and visit_factor > 0 else SCRAPE_VISIT_FACTOR
    effective_max_visits: Optional[int] = max_visits if max_visits and max_visits > 0 else SCRAPE_MAX_VISITS
    if effective_max_visits is None and target_pages is not None and effective_visit_factor:
        effective_max_visits = max(target_pages * effective_visit_factor, target_pages)
    headers = {"User-Agent": USER_AGENT}
    concurrency = max(1, concurrency)
    queue_limit = effective_max_visits * 2 if effective_max_visits else QUEUE_LIMIT_FALLBACK

    async def _visit_page(session: aiohttp.ClientSession, cur: str) -> Tuple[str, str, str, Optional[object]]:
        html = await _fetch_text(session, cur)
        title, content, soup = _extract_article(html, cur)
        return title, content, html, soup

    async with aiohttp.ClientSession(headers=headers, timeout=DEFAULT_TIMEOUT) as session:
        while queue and (target_pages is None or len(results) < target_pages) and (
            effective_max_visits is None or visits < effective_max_visits
        ):
            batch: List[str] = []
            while queue and len(batch) < concurrency and (
                effective_max_visits is None or visits + len(batch) < effective_max_visits
            ):
                cur = queue.popleft()
                if cur in seen:
                    continue
                seen.add(cur)
                batch.append(cur)

            if not batch:
                break

            tasks = [asyncio.create_task(_visit_page(session, cur)) for cur in batch]
            outcomes = await asyncio.gather(*tasks, return_exceptions=True)

            for cur, outcome in zip(batch, outcomes):
                visits += 1
                if isinstance(outcome, Exception):
                    continue

                title, content, html, soup = outcome
                if title and (
                    len(content) >= min_content_len
                    or (len(content) > 30 and len(content) < min_content_len)
                ):
                    results.append({"url": cur, "title": title, "content": content})
                    if target_pages is not None and len(results) >= target_pages:
                        break

                # Enlaces del mismo dominio
                if soup is None:
                    try:
                        soup = make_soup(html)
                    except Exception:
                        soup = None
                if soup is None:
                    continue
                try:
                    for a in soup.find_all("a", href=True):
                        nu = _normalize_url(cur, a["href"])
                        if not nu:
                            continue
                        if not _same_domain(url, nu):
                            continue
                        if nu not in seen and len(queue) < queue_limit:
                            queue.append(nu)
                except Exception:
                    continue

    return results