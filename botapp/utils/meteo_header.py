from __future__ import annotations
from pathlib import Path
import re
import asyncio
from ..config import get_settings
from ..utils.time import today_str
from ..services.weather import get_weather_block
from ..utils.operational_day import opday_today_str

SET = get_settings()

METEO_START = r"^=== METEO .* ===$"
METEO_END   = r"^=== FIN METEO ===$"

def _today_file(country: str) -> Path:
    d = Path(SET.data_dir) / country.lower()
    d.mkdir(parents=True, exist_ok=True)
    opday = opday_today_str(SET.tz)          # <--- clave: día operativo
    return d / f"{opday}.txt"

def _has_meteo_block(text: str) -> bool:
    start = re.search(METEO_START, text, flags=re.MULTILINE)
    end = re.search(METEO_END, text, flags=re.MULTILINE)
    return bool(start and end and start.start() < end.start())

async def prepend_weather_header(country: str) -> Path:
    """
    Si el TXT de hoy no tiene bloque METEO, lo prepende automáticamente.
    Devuelve la ruta del archivo.
    """
    f = _today_file(country)
    if not f.exists():
        f.write_text("", encoding="utf-8")

    content = f.read_text(encoding="utf-8")
    if _has_meteo_block(content):
        return f

    # Construir bloque meteo (AEMET/OWM)
    block = await get_weather_block(country)

    # Prependemos
    updated = (block + content) if content else block
    f.write_text(updated, encoding="utf-8")
    return f