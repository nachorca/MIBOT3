# botapp/services/notam.py
from __future__ import annotations

from datetime import date
from pathlib import Path


# Raíz de MIBOT3: dos niveles por encima de este archivo
BASE_DIR = Path(__file__).resolve().parents[2]
NOTAM_DIR = BASE_DIR / "data" / "notam"


def _ensure_notam_dir() -> None:
    """
    Asegura que existe la carpeta data/notam.
    """
    NOTAM_DIR.mkdir(parents=True, exist_ok=True)


def _build_notam_filename(
    country_code: str,
    icao: str,
    report_date: date,
) -> Path:
    """
    Genera un nombre de archivo del tipo:
        data/notam/HT_MTPP_2025-11-24.txt
    """
    safe_country = country_code.upper()
    safe_icao = icao.upper()
    filename = f"{safe_country}_{safe_icao}_{report_date.isoformat()}.txt"
    return NOTAM_DIR / filename


def get_notam_summary(
    country_code: str,
    icao: str,
    report_date: date | None = None,
) -> str:
    """
    Devuelve el texto 'cuerpo' del NOTAM desde un TXT en data/notam, si existe.
    Si no existe, devuelve una plantilla por defecto.

    El archivo esperado será:
        data/notam/<COUNTRY>_<ICAO>_<YYYY-MM-DD>.txt

    Ejemplo Haití:
        data/notam/HT_MTPP_2025-11-24.txt

    Dentro del TXT tú puedes escribir directamente las líneas tipo:
        • Estado operativo: ABIERTO / CERRADO / OPERACIÓN RESTRINGIDA
        • NOTAM relevantes:
           – {Código NOTAM} – {Resumen traducido al español}
        • Impacto operativo:
           – {impacto para vuelos ONU/ONGs, evacuaciones, logística}
    """
    _ensure_notam_dir()

    if report_date is None:
        report_date = date.today()

    path = _build_notam_filename(country_code, icao, report_date)

    if not path.exists():
        # Plantilla genérica si no hay archivo
        return (
            "• Estado operativo: ABIERTO / CERRADO / OPERACIÓN RESTRINGIDA\n"
            "• NOTAM relevantes:\n"
            "   – N/A – No se ha cargado un resumen específico para hoy.\n"
            "• Impacto operativo:\n"
            "   – Revisar manualmente la operatividad del aeropuerto para\n"
            "     vuelos ONU/ONGs, evacuaciones y logística.\n"
        )

    text = path.read_text(encoding="utf-8").strip()
    if not text:
        return (
            "• El archivo NOTAM existe pero está vacío. "
            "Revisar contenido en data/notam.\n"
        )

    return text + ("\n" if not text.endswith("\n") else "")


def build_notam_block(
    airport_name: str,
    icao: str,
    country_code: str,
    report_date: date | None = None,
) -> str:
    """
    Construye el bloque completo, con el formato:

    ✈️ NOTAM – Aeropuerto Internacional Toussaint Louverture (MTPP)

    • Estado operativo: ABIERTO / CERRADO / OPERACIÓN RESTRINGIDA
    • NOTAM relevantes:
       – {Código NOTAM} – {Resumen traducido al español}
    • Impacto operativo:
       – {impacto para vuelos ONU/ONGs, evacuaciones, logística}
    """
    if report_date is None:
        report_date = date.today()

    summary = get_notam_summary(
        country_code=country_code,
        icao=icao,
        report_date=report_date,
    )

    header = f"✈️ NOTAM – {airport_name} ({icao.upper()})\n\n"
    return header + summary