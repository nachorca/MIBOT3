# -*- coding: utf-8 -*-
import re
from typing import List, Dict, Any, Optional

# Cabeceras válidas (normalizamos a la categoría SICU canónica)
SICU_SECCIONES = {
    "conflicto armado": "Conflicto Armado",
    "terrorismo": "Terrorismo",
    "delincuencia": "Criminalidad",
    "criminalidad": "Criminalidad",
    "disturbios civiles": "Disturbios Civiles",
    "hazards": "Hazards",
}

# Viñetas tipo lista
BULLET_RE = re.compile(r"^\s*[-•*]\s+(?P<texto>.+)$", re.IGNORECASE)

# Cabeceras de sección (línea completa)
HEADER_RE = re.compile(
    r"^\s*(conflicto armado|terrorismo|delincuencia|criminalidad|disturbios civiles|hazards)\s*:?\s*$",
    re.IGNORECASE
)

# patrones simples para extraer 'place' del texto del incidente
PLACE_CANDIDATE_RE = [
    re.compile(r"\ben\s+([A-ZÁÉÍÓÚÜÑ][\w\-\s'’\.]+)", re.IGNORECASE),
    re.compile(r"\ben la zona de\s+([A-ZÁÉÍÓÚÜÑ][\w\-\s'’\.]+)", re.IGNORECASE),
    re.compile(r"\ben el distrito de\s+([A-ZÁÉÍÓÚÜÑ][\w\-\s'’\.]+)", re.IGNORECASE),
]

def _normaliza_sicu(seccion: str) -> str:
    key = seccion.strip().lower()
    return SICU_SECCIONES.get(key, "Otros")

def _extrae_place(texto: str) -> Optional[str]:
    for rx in PLACE_CANDIDATE_RE:
        m = rx.search(texto)
        if m:
            place = m.group(1).strip().rstrip(".,;: ")
            return place
    return None

def parse_incidents_from_text(texto: str, default_fuente: str = "Informe") -> List[Dict[str, Any]]:
    """
    Lee un texto con secciones SICU y viñetas.
    Devuelve: [{categoria, descripcion, place, fuente}, ...]
    """
    incidentes: List[Dict[str, Any]] = []
    seccion_actual: Optional[str] = None

    for raw_line in texto.splitlines():
        line = raw_line.strip()
        if not line:
            continue

        # ¿Cabecera de sección SICU?
        h = HEADER_RE.match(line)
        if h:
            seccion_actual = _normaliza_sicu(h.group(1))
            continue

        # ¿Viñeta (incidente)?
        m = BULLET_RE.match(line)
        if m and seccion_actual:
            texto_inc = m.group("texto").strip()
            place = _extrae_place(texto_inc)
            incidentes.append({
                "categoria": seccion_actual,
                "descripcion": texto_inc,
                "place": place,
                "fuente": default_fuente,
            })

    return incidentes
