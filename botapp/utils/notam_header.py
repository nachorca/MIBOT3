from __future__ import annotations
from pathlib import Path
import re

from ..config import get_settings
from ..utils.operational_day import opday_today_str

# Intentamos importar fetch_notams del paso 1 (services/notam.py).
# Si aún no existe, dejamos un stub claro para que el desarrollador lo implemente.
try:
    from ..services.notam import fetch_notams  # async def fetch_notams(icao: str) -> list[dict]
except Exception:  # pragma: no cover
    async def fetch_notams(_icao: str):
        raise RuntimeError(
            "fetch_notams no disponible: implementa botapp/services/notam.py (paso 1) "
            "con una función async fetch_notams(icao: str) -> list[dict] que devuelva NOTAMs."
        )

SET = get_settings()

# Delimitadores del bloque NOTAM en los TXT
NOTAM_START = r"^=== NOTAM .* ===$"
NOTAM_END   = r"^=== FIN NOTAM ===$"

def _today_file(country: str) -> Path:
    """
    Devuelve la ruta del TXT del día operativo actual para el país indicado.
    Crea la carpeta si no existe.
    """
    d = Path(SET.data_dir) / country.lower()
    d.mkdir(parents=True, exist_ok=True)
    opday = opday_today_str(SET.tz)
    return d / f"{opday}.txt"

def _has_notam_block(text: str) -> bool:
    """
    Devuelve True si ya existe un bloque NOTAM en el texto.
    Requiere encontrar cabecera y pie en orden.
    """
    start = re.search(NOTAM_START, text, flags=re.MULTILINE)
    end = re.search(NOTAM_END, text, flags=re.MULTILINE)
    return bool(start and end and start.start() < end.start())

async def prepend_notam_header(icao: str, country: str) -> Path:
    """
    Inserta (prepend) un bloque NOTAM para el ICAO indicado al inicio del TXT
    del día operativo de 'country', si aún no existe.
    - icao: código ICAO (p.ej. MTPP, HLLM)
    - country: país/carpeta del TXT (p.ej. 'haiti', 'libia', 'colombia')
    """
    f = _today_file(country)
    if not f.exists():
        f.write_text("", encoding="utf-8")

    content = f.read_text(encoding="utf-8")
    if _has_notam_block(content):
        # Ya existe un bloque NOTAM; no duplicamos
        return f

    # Obtener NOTAMs (usa el paso 1: services/notam.fetch_notams)
    notams = await fetch_notams(icao)

    # Construcción del bloque
    lines = [f"=== NOTAM {icao.upper()} ==="]
    if not notams:
        lines.append("No hay NOTAM disponibles.")
    else:
        # Cada item debería tener 'raw' (cadena NOTAM completa). Ajusta si tu fetch devuelve otro formato.
        for n in notams:
            raw = n.get("raw") if isinstance(n, dict) else str(n)
            if raw:
                lines.append(raw)
    lines.append("=== FIN NOTAM ===")
    lines.append("")  # salto de línea extra

    new_content = "\n".join(lines) + content
    f.write_text(new_content, encoding="utf-8")
    return f
