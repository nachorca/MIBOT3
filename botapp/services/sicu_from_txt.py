# -*- coding: utf-8 -*-
"""
sicu_from_txt.py – MIBOT3

Convierte un feed operacional (TXT) en:
  - CSV SICU
  - KML SICU con colores estándar

Integrable en handlers MIBOT3:
  from botapp.services.sicu_from_txt import parse_sicu_from_txt, generate_sicu_csv, generate_sicu_kml
"""

from __future__ import annotations
import csv
from pathlib import Path
from dataclasses import dataclass
from typing import List, Optional, Tuple, Dict
from xml.etree.ElementTree import Element, SubElement, tostring
from xml.dom.minidom import parseString


# ==========================================
# 1. CONFIGURACIÓN DE LOCALIZACIONES (GAZA)
#    → Luego podrás sustituir por tu Gazetteer
# ==========================================

GAZA_LOCATIONS: Dict[str, Tuple[float, float]] = {
    "shuja": (31.52, 34.48),
    "shujaiya": (31.52, 34.48),
    "tuffah": (31.52, 34.49),
    "zaytoun": (31.51, 34.47),
    "beit lahiya": (31.55, 34.49),
    "as-salatin": (31.55, 34.47),
    "jabalia": (31.55, 34.50),
    "bureij": (31.44, 34.39),
    "maghazi": (31.43, 34.40),
    "deir al-balah": (31.42, 34.35),
    "khan younis": (31.34, 34.30),
    "bani suhaila": (31.34, 34.36),
    "qizan al-najjar": (31.33, 34.32),
    "rafah": (31.28, 34.25),
    "mawasi": (31.29, 34.23),
    "gaza city": (31.52, 34.46),
}

# COLORES SICU (formato KML = ABGR)
KML_COLORS = {
    "Conflicto Armado": "ff0000ff",       # rojo
    "Terrorismo":       "ff000000",       # negro
    "Criminalidad":     "ffff0000",       # azul
    "Disturbios Civiles": "ff00ffff",     # amarillo
    "Hazards":          "ff00a5ff",       # naranja
}


# ==========================================
# 2. DATACLASS PARA INCIDENTES SICU
# ==========================================

@dataclass
class Incident:
    num: int
    fecha: str
    hora: str
    localizacion: str
    categoria: str
    breve: str
    subcategoria: str
    severidad: str
    lat: Optional[float]
    lon: Optional[float]
    fuente: str


# ==========================================
# 3. DETECCIÓN Y CLASIFICACIÓN SICU
# ==========================================

def guess_category(text: str) -> Optional[str]:
    t = text.lower()

    # Conflicto armado
    if any(w in t for w in [
        "artiller", "bombarde", "airstrike", "drone", "dron", "quad",
        "helicóp", "helico", "tanque", "fuego", "sniper", "misil",
        "línea amarilla", "franja amarilla", "explos", "demolic"
    ]):
        return "Conflicto Armado"

    # Hazards
    if any(w in t for w in [
        "inund", "frío", "anemia", "hambruna", "colapso", "hospital",
        "sanit", "uxo", "muse", "asbesto", "escombro"
    ]):
        return "Hazards"

    # Criminalidad
    if any(w in t for w in ["robo", "saqueo", "contrabando", "extorsión"]):
        return "Criminalidad"

    # Disturbios Civiles
    if any(w in t for w in ["protest", "disturb", "manifest", "bloqueo"]):
        return "Disturbios Civiles"

    # Terrorismo
    if "atentado" in t or "terror" in t:
        return "Terrorismo"

    return None


def guess_location(text: str) -> str:
    t = text.lower()
    for key in GAZA_LOCATIONS.keys():
        if key in t:
            return key.title()
    return "Gaza (general)"


def guess_latlon(loc: str) -> Tuple[Optional[float], Optional[float]]:
    key = loc.lower()
    for k, coords in GAZA_LOCATIONS.items():
        if k in key:
            return coords
    return None, None


def guess_subcat_and_sev(text: str, cat: str) -> Tuple[str, str]:
    t = text.lower()
    sub = "Sin especificar"
    sev = "Moderado"

    if cat == "Conflicto Armado":
        if "drone" in t or "dron" in t or "quad" in t:
            sub = "Ataque con UAV"
            sev = "Alto"
        if "niño" in t or "menor" in t:
            sev = "Crítico"
        if "artiller" in t:
            sub = "Fuego indirecto"
            sev = "Alto"
        if "demolic" in t or "voladur" in t:
            sub = "Demolición dirigida"
            sev = "Crítico"
        if "muere" in t or "mártir" in t or "falleci" in t:
            sub = "Letalidad por fuego directo"
            sev = "Crítico"

    elif cat == "Hazards":
        if "colapso" in t or "hospital" in t:
            sub = "Colapso sanitario"
            sev = "Crítico"
        if "inund" in t or "tienda" in t:
            sub = "Inundación campamentos"
            sev = "Crítico"
        if "anemia" in t or "hambruna" in t:
            sub = "Crisis nutricional"
            sev = "Crítico"

    elif cat == "Criminalidad":
        sub = "Crimen común"
        sev = "Moderado"

    elif cat == "Disturbios Civiles":
        sub = "Protestas / enfrentamientos"
        sev = "Bajo"

    elif cat == "Terrorismo":
        sub = "Ataque terrorista"
        sev = "Crítico"

    return sub, sev


def is_incident_line(text: str) -> bool:
    t = text.strip()
    if not t:
        return False
    if t.startswith("===") or t.lower().startswith("meteo"):
        return False
    if any(w in t.lower() for w in [
        "artiller", "bombarde", "drone", "dron", "quad", "tanque",
        "disparo", "muere", "mártir", "herido", "inund", "colapso",
        "anemia", "hospital", "frente frío", "muse", "uxo"
    ]):
        return True
    return False


# ==========================================
# 4. PARSEO PRINCIPAL
# ==========================================

def parse_sicu_from_txt(text: str, fecha: str) -> List[Incident]:
    incidents: List[Incident] = []
    count = 1

    for line in text.splitlines():
        line = line.strip()
        if not is_incident_line(line):
            continue

        cat = guess_category(line)
        if not cat:
            continue

        loc = guess_location(line)
        lat, lon = guess_latlon(loc)
        sub, sev = guess_subcat_and_sev(line, cat)

        incidents.append(
            Incident(
                num=count,
                fecha=fecha,
                hora="",
                localizacion=loc,
                categoria=cat,
                breve=line,
                subcategoria=sub,
                severidad=sev,
                lat=lat,
                lon=lon,
                fuente="Feed operacional"
            )
        )
        count += 1

    return incidents


# ==========================================
# 5. GENERAR CSV
# ==========================================

def generate_sicu_csv(incidents: List[Incident], out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "Nº","Fecha","Hora","Localización","Categoría SICU",
            "Breve descripción (en español)","Subcategoría",
            "Nivel de severidad","Lat","Lon","Fuente"
        ])
        for i in incidents:
            writer.writerow([
                i.num, i.fecha, i.hora, i.localizacion, i.categoria,
                i.breve, i.subcategoria, i.severidad, i.lat, i.lon, i.fuente
            ])


# ==========================================
# 6. GENERAR KML
# ==========================================

def generate_sicu_kml(incidents: List[Incident], out_path: Path, nombre_documento: str):
    out_path.parent.mkdir(parents=True, exist_ok=True)

    kml = Element("kml", xmlns="http://www.opengis.net/kml/2.2")
    doc = SubElement(kml, "Document")
    SubElement(doc, "name").text = nombre_documento

    # Estilos
    for cat, color in KML_COLORS.items():
        st = SubElement(doc, "Style", id=cat.replace(" ", "_"))
        icon = SubElement(st, "IconStyle")
        SubElement(icon, "color").text = color
        SubElement(icon, "scale").text = "1.2"
        ih = SubElement(icon, "Icon")
        SubElement(ih, "href").text = "http://maps.google.com/mapfiles/kml/shapes/placemark_circle.png"

    # Puntos
    for inc in incidents:
        if inc.lat is None or inc.lon is None:
            continue

        pm = SubElement(doc, "Placemark")
        SubElement(pm, "name").text = f"{inc.localizacion} ({inc.categoria})"
        SubElement(pm, "styleUrl").text = "#" + inc.categoria.replace(" ", "_")
        desc = f"{inc.breve} – Subcat: {inc.subcategoria} – Severidad: {inc.severidad}"
        SubElement(pm, "description").text = desc
        pt = SubElement(pm, "Point")
        SubElement(pt, "coordinates").text = f"{inc.lon},{inc.lat},0"

    xml = parseString(tostring(kml)).toprettyxml(indent="  ")
    out_path.write_text(xml, encoding="utf-8")


# ==========================================
# READY PARA IMPORTAR EN HANDLERS MIBOT3
# ==========================================

__all__ = [
    "parse_sicu_from_txt",
    "generate_sicu_csv",
    "generate_sicu_kml"
]