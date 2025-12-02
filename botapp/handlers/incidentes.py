# -*- coding: utf-8 -*-
import asyncio
import csv
import re
from datetime import datetime
from pathlib import Path
from typing import Optional

from telegram import Update, InputFile
from telegram.ext import ContextTypes

from botapp.config import get_settings
from botapp.services.incidentes_resolver import resolve_missing_coords
from botapp.services.geocoder import geocode_place
from botapp.utils.translator import to_spanish_excerpt
from mgrs import MGRS

SETTINGS = get_settings()
DATA_DIR = Path(SETTINGS.data_dir)
OUTPUT_DIR = (DATA_DIR.parent if DATA_DIR.parent != DATA_DIR else Path(".")) / "output" / "incidentes"
MGRS_CONVERTER = MGRS()

INCIDENT_FIELDS = [
    "id",
    "pais",
    "categoria",
    "descripcion",
    "fuente",
    "lat",
    "lon",
    "mgrs",
    "place",
    "admin1",
    "admin2",
    "accuracy",
    "geocode_source",
    "created_at",
    "updated_at",
]

CATEGORY_KEYWORDS = [
    (
        "Terrorismo",
        (
            "terrorismo",
            "terrorist",
            "ied",
            "artefacto explosivo improvisado",
            "suicide bomb",
            "bomba suicida",
            "car bomb",
            "explosive device",
        ),
    ),
    (
        "Conflicto Armado",
        (
            "enfrentamiento",
            "combate",
            "shelling",
            "artillery",
            "bombardeo",
            "clashes",
            "firefight",
            "ataque aéreo",
            "airstrike",
            "misil",
            "drone strike",
        ),
    ),
    (
        "Criminalidad",
        (
            "criminal",
            "delincuencia",
            "robbery",
            "asalto",
            "secuestro",
            "kidnap",
            "extorsión",
            "narco",
            "smuggling",
            "homicidio",
            "murder",
            "shooting",
        ),
    ),
    (
        "Disturbios Civiles",
        (
            "protesta",
            "protest",
            "manifestación",
            "manifestacion",
            "riot",
            "huelga",
            "strike",
            "bloqueo",
            "roadblock",
            "march",
        ),
    ),
    (
        "Hazards",
        (
            "inundacion",
            "inundación",
            "flood",
            "tormenta",
            "storm",
            "huracán",
            "hurricane",
            "terremoto",
            "earthquake",
            "deslizamiento",
            "landslide",
            "incendio",
            "fire",
        ),
    ),
]

ENTRY_RE = re.compile(
    r"^---\s*(?P<channel>.+?)\s*@\s*(?P<dt>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\s*---\s*$"
)

_WS_RE = re.compile(r"\s+")
_HASHTAG_RE = re.compile(r"#([^\s#.,;:]+)")
_LOCATION_MAIN_PATTERNS = [
    re.compile(r"\b(?:en|en la|en el|en los|en las|in|at|within|inside)\s+([A-ZÁÉÍÓÚÜÑ][^.,;\n]+)", re.IGNORECASE),
    re.compile(
        r"\b(?:cerca de|en las cercan[ií]as de|en las proximidades de|pr[oó]ximo a|alrededor de|junto a|"
        r"cerca|near|around|outside|west of|east of|south of|north of)\s+([A-ZÁÉÍÓÚÜÑ][^.,;\n]+)",
        re.IGNORECASE,
    ),
    re.compile(
        r"\b(?:al|a la|a los|a las|towards)\s+(?:norte|sur|este|oeste|sureste|suroeste|noreste|noroeste|"
        r"north|south|east|west|northwest|northeast|southwest|southeast)\s+de\s+([A-ZÁÉÍÓÚÜÑ][^.,;\n]+)",
        re.IGNORECASE,
    ),
    re.compile(r"^\s*([A-ZÁÉÍÓÚÜÑ][\w\s'’\-\(\)\/]+?)\s*[:\-–]\s", re.MULTILINE),
    re.compile(r"\b([A-ZÁÉÍÓÚÜÑ][\w\s'’\-\(\)\/]+?\([^)]+\))"),
]
_LOCATION_FALLBACK_RE = re.compile(
    r"([A-ZÁÉÍÓÚÜÑ][\w'’\-]+(?:\s+[A-ZÁÉÍÓÚÜÑ][\w'’\-]+){0,3})",
)
_GENERIC_TAGS = {
    "libia",
    "libya",
    "news",
    "noticias",
    "breaking",
    "ultimahora",
    "alerta",
    "urgent",
    "breakingnews",
    "ultima",
    "última",
    "aljazeera",
    "الجزيرة",
    "occidental",
    "oriental",
    "meridional",
    "septentrional",
    "hamas",
}
_FALLBACK_EXCLUDE = {
    "ministerio",
    "gobierno",
    "presidente",
    "ministro",
    "defensa",
    "fuerzas",
    "ejército",
    "army",
    "forces",
    "breaking",
    "urgent",
    "occidental",
    "oriental",
    "meridional",
    "septentrional",
    "seguridad",
    "ataques",
    "taller",
    "fuera",
}
_SOURCE_PREFIXES = (
    "primer ministro",
    "ministro",
    "ministerio",
    "presidente",
    "canal",
    "reuters",
    "agencia",
    "oficina",
    "fuente",
    "ejército",
    "army",
    "forces",
    "taller",
)

_DIRECTION_PREFIX_RE = re.compile(
    r"^(?:al|a la|a los|a las)\s+(?:norte|sur|este|oeste|"
    r"noreste|noroeste|sureste|suroeste|centro|nordeste|sudeste|sudoeste)\s+de\s+",
    re.IGNORECASE,
)
_NEAR_PREFIX_RE = re.compile(
    r"^(?:cerca de|cercan[ií]as de|en las cercan[ií]as de|en las proximidades de|pr[oó]ximo a|alrededor de|"
    r"junto a|near|around|outside|by)\s+",
    re.IGNORECASE,
)
_ARTICLE_PREFIX_RE = re.compile(r"^(?:la|el|los|las|the)\s+", re.IGNORECASE)
_PLACE_PREFIX_RE = re.compile(
    r"^(?:ciudad|city|pueblo|town|provincia|province|estado|state|departamento|department|"
    r"region|región|distrito|district|gobernación|governorate)\s+(?:de\s+)?",
    re.IGNORECASE,
)
_OF_PREFIX_RE = re.compile(r"^(?:of|de|del|de la|de los|de las)\s+", re.IGNORECASE)
_PLACE_SUFFIX_RE = re.compile(
    r"\s+(?:city|ciudad|province|provincia|state|estado|region|región|district|distrito|governorate|gobierno)$",
    re.IGNORECASE,
)
_BULLET_RE = re.compile(r"^[•●]\s*")
_TRAILING_PUNCT_RE = re.compile(r"[\"'”’)\]]+$")
_LEADING_PUNCT_RE = re.compile(r"^[\"'“‘(\[]+")
_KNOWN_PLACE_REWRITES = {
    "gaza strip": "Gaza Strip",
    "gaza city": "Gaza City",
    "tripoli libya": "Tripoli, Libya",
}


def _clean_location_token(token: str) -> str:
    token = token.strip()
    token = _BULLET_RE.sub("", token)
    token = _LEADING_PUNCT_RE.sub("", token)
    token = _TRAILING_PUNCT_RE.sub("", token)
    token = token.replace("_", " ")
    token = _WS_RE.sub(" ", token).strip()
    token = _DIRECTION_PREFIX_RE.sub("", token)
    token = _ARTICLE_PREFIX_RE.sub("", token)
    token = _NEAR_PREFIX_RE.sub("", token)
    token = _PLACE_PREFIX_RE.sub("", token)
    token = _OF_PREFIX_RE.sub("", token)
    token = _PLACE_SUFFIX_RE.sub("", token)
    lowered = token.lower()
    if lowered in _KNOWN_PLACE_REWRITES:
        token = _KNOWN_PLACE_REWRITES[lowered]
        lowered = token.lower()

    for stopper in (
        " que ",
        " ha ",
        " han ",
        " está ",
        " están ",
        " estan ",
        " será ",
        " seran ",
        " serán ",
        " fueron ",
        " informan ",
        " indicando ",
    ):
        idx = lowered.find(stopper)
        if idx > 3:
            token = token[:idx].strip(" ,.;:-")
            lowered = token.lower()

    for stopper in (" en el ", " en la ", " en los ", " en las "):
        idx = lowered.find(stopper)
        if idx > 3:
            token = token[:idx].strip(" ,.;:-")
            lowered = token.lower()

    return token


def _extract_location(*texts: Optional[str]) -> str:
    """
    Intenta obtener una localización de los textos proporcionados (original y/o traducido).
    Prioriza patrones lingüísticos explícitos; como fallback usa hashtags.
    """
    seen: set[str] = set()
    candidates: list[str] = []

    def _push(raw: str) -> None:
        cleaned = _clean_location_token(raw)
        if not cleaned:
            return
        lowered = cleaned.lower()
        if cleaned.isupper() and len(cleaned) > 3:
            cleaned = cleaned.title()
            lowered = cleaned.lower()
        if lowered in _GENERIC_TAGS or lowered in seen:
            return
        if lowered.startswith(("que ", "el que ", "la que ", "los que ", "las que ")):
            return
        if any(lowered.startswith(prefix) for prefix in _SOURCE_PREFIXES):
            return
        if " informan " in lowered:
            return
        if len(cleaned) <= 2 or lowered in {"los", "las", "el", "la", "lo"}:
            return
        if cleaned.isupper() and len(cleaned) <= 4:
            return
        seen.add(lowered)
        candidates.append(cleaned)

    for text in texts:
        if not text:
            continue
        for pattern in _LOCATION_MAIN_PATTERNS:
            match = pattern.search(text)
            if match:
                _push(match.group(1))
        if candidates:
            break

        for tag in _HASHTAG_RE.findall(text):
            candidate = tag.lstrip("#")
            _push(candidate)
        if candidates:
            break

        # Fallback: busca secuencias Capitalizadas limitadas
        for match in _LOCATION_FALLBACK_RE.finditer(text):
            chunk = match.group(1)
            if len(chunk) < 3:
                continue
            if chunk.isupper() and len(chunk.split()) == 1:
                continue
            words = {w.lower() for w in chunk.split()}
            if words & _FALLBACK_EXCLUDE:
                continue
            _push(chunk)
            if candidates:
                break

    return candidates[0] if candidates else ""


def _normalize_key(*parts: str) -> str:
    joined = " ".join(str(p or "") for p in parts)
    return _WS_RE.sub(" ", joined).strip().lower()


def _classify_categoria(text: str) -> str:
    """
    Clasificación heurística basada en palabras clave.
    """
    if not text:
        return "Sin clasificar"
    lowered = text.lower()
    for categoria, keywords in CATEGORY_KEYWORDS:
        if any(keyword in lowered for keyword in keywords):
            return categoria
    return "Sin clasificar"


def _latlon_to_mgrs(lat: float | str | None, lon: float | str | None) -> str:
    """
    Convierte pares lat/lon en formato MGRS si están dentro de los rangos válidos.
    """
    try:
        lat_f = float(lat)  # type: ignore[arg-type]
        lon_f = float(lon)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return ""
    if not (-80.0 <= lat_f <= 84.0):
        return ""
    try:
        return MGRS_CONVERTER.toMGRS(lat_f, lon_f)
    except Exception:
        return ""


async def _populate_geodata(records: list[dict]) -> None:
    """
    Enriquecer registros con lat/lon/admin mediante geocodificación síncrona en un hilo aparte.
    """
    for row in records:
        if row.get("lat") and row.get("lon"):
            continue
        place = row.get("place")
        if not place:
            continue
        country_hint = row.get("_country_hint") or row.get("pais")
        try:
            result = await asyncio.to_thread(geocode_place, place, country_hint)
        except Exception:
            result = None
        if not result:
            continue
        lat, lon, admin1, admin2, accuracy, source = result
        row["lat"] = f"{lat:.6f}"
        row["lon"] = f"{lon:.6f}"
        row["admin1"] = admin1 or row.get("admin1", "")
        row["admin2"] = admin2 or row.get("admin2", "")
        row["accuracy"] = accuracy or row.get("accuracy", "")
        row["geocode_source"] = source or row.get("geocode_source", "")


def _parse_message_entries(text: str):
    """
    Convierte un TXT de recolección en entradas [{channel, dt, text}, ...].
    Ignora el prefijo antes del primer header válido.
    """
    entries = []
    current = None
    buffer: list[str] = []

    for raw_line in text.splitlines():
        match = ENTRY_RE.match(raw_line)
        if match:
            if current:
                current["text"] = "\n".join(buffer).strip()
                entries.append(current)
                buffer = []
            current = {
                "channel": match.group("channel").strip(),
                "dt": match.group("dt"),
            }
            continue

        if current is None:
            continue  # aún no hay entrada, ignorar cabeceras METEO/EXCHANGE, etc.
        buffer.append(raw_line)

    if current:
        current["text"] = "\n".join(buffer).strip()
        entries.append(current)

    return entries

async def incidentes_resolve(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/incidentes_resolve [Pais]  — geocodifica pendientes."""
    hint = None
    if context.args and len(context.args) > 0:
        hint = " ".join(context.args).strip()
    n = resolve_missing_coords(default_country_hint=hint)
    await update.message.reply_text(f"✅ Resueltos {n} incidentes pendientes.")


async def incidentes_csv(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /incidentes_csv <pais|all> <YYYY-MM-DD>
    Lee los TXT recolectados y genera un CSV con los mensajes del día indicado.
    • pais=ALL/TODOS procesa todos los países que tengan archivo para esa fecha.
    • La fecha corresponde al archivo YYYY-MM-DD.txt dentro del directorio del país.
    """
    if len(context.args) < 2:
        return await update.message.reply_text(
            "Uso: /incidentes_csv <pais|all> <YYYY-MM-DD>\n"
            "Ej: /incidentes_csv libia 2024-10-31"
        )

    raw_country_arg = context.args[0].strip()
    country_arg = raw_country_arg.lower()
    if country_arg in ("all", "todos", "todo", "global"):
        country_filter = None
        slug = "todos"
        scope_label = "todos los países"
    else:
        country_filter = country_arg
        slug = country_arg.replace(" ", "_")
        scope_label = raw_country_arg

    day_arg = context.args[1].strip()
    try:
        datetime.strptime(day_arg, "%Y-%m-%d")
    except ValueError:
        return await update.message.reply_text("La fecha debe tener formato YYYY-MM-DD.")

    selected: list[tuple[str, str]] = []
    if country_filter is None:
        for path in sorted(DATA_DIR.iterdir()):
            if not path.is_dir():
                continue
            if path.name.startswith("."):
                continue
            selected.append((path.name, path.name))
        scope_label = "todos los países"
    else:
        selected.append((country_filter, raw_country_arg))

    records = []
    missing_files = []
    seen_keys: set[str] = set()
    for slug_name, display in selected:
        fpath = DATA_DIR / slug_name / f"{day_arg}.txt"
        if not fpath.exists():
            missing_files.append(display)
            continue

        try:
            content = fpath.read_text(encoding="utf-8")
        except Exception:
            content = fpath.read_text(encoding="utf-8", errors="ignore")

        for entry in _parse_message_entries(content):
            original = entry.get("text", "")
            if not original:
                continue
            body = to_spanish_excerpt(original)
            if not body:
                body = original.strip()
            dt_raw = entry.get("dt")
            try:
                dt_obj = datetime.strptime(dt_raw, "%Y-%m-%d %H:%M:%S") if dt_raw else None
            except ValueError:
                dt_obj = None
            timestamp = dt_obj.isoformat() if dt_obj else (dt_raw or "")

            place = _extract_location(body, original)
            dedupe_key = _normalize_key(body or original)
            if not dedupe_key:
                continue
            if dedupe_key in seen_keys:
                continue
            seen_keys.add(dedupe_key)

            categoria = _classify_categoria(body)
            records.append({
                "id": len(records) + 1,
                "pais": display,
                "categoria": categoria,
                "descripcion": body,
                "fuente": entry.get("channel", ""),
                "lat": "",
                "lon": "",
                "mgrs": "",
                "place": place,
                "admin1": "",
                "admin2": "",
                "accuracy": "",
                "geocode_source": "mensajes",
                "created_at": timestamp,
                "updated_at": timestamp,
                "_country_hint": slug_name,
            })

    if not records:
        note = ""
        if missing_files:
            note = f" (sin archivo {day_arg}.txt para: {', '.join(missing_files)})"
        return await update.message.reply_text(
            f"No se encontraron mensajes para {scope_label} el {day_arg}.{note}"
        )

    await _populate_geodata(records)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    csv_path = OUTPUT_DIR / f"incidentes_{slug}_{day_arg}_{timestamp}.csv"

    with csv_path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=INCIDENT_FIELDS, extrasaction="ignore")
        writer.writeheader()
        for row in records:
            row.pop("_country_hint", None)
            if not row.get("mgrs"):
                row["mgrs"] = _latlon_to_mgrs(row.get("lat"), row.get("lon"))
            writer.writerow({field: row.get(field, "") for field in INCIDENT_FIELDS})

    with csv_path.open("rb") as fh:
        await update.message.reply_document(
            document=InputFile(fh, filename=csv_path.name),
            caption=(
                f"📄 Incidentes exportados ({len(records)} registros)\n"
                f"País: {scope_label}\n"
                f"Día: {day_arg}\n"
                f"Ruta: {csv_path}"
            )
        )

    if missing_files:
        await update.message.reply_text(
            f"⚠️ Sin archivo {day_arg}.txt para: {', '.join(missing_files)}. Solo se exportaron países disponibles."
        )
