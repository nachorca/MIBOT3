# -*- coding: utf-8 -*-
# botapp/handlers/incidentes_categorizados.py
from __future__ import annotations
from telegram import Update, InputFile
from telegram.ext import ContextTypes
from telegram.error import RetryAfter
from pathlib import Path
import csv
import unicodedata
from typing import Dict, Any, List
import asyncio

from botapp.config import get_settings

SET = get_settings()

# Rutas base
DATA_DIR = Path(SET.data_dir).resolve()              # ./data
PROJECT_ROOT = DATA_DIR.parent if DATA_DIR.parent != DATA_DIR else Path(__file__).resolve().parents[2]
OUTPUT_DIR = PROJECT_ROOT / "output"
INCIDENTS_DIR = OUTPUT_DIR / "incidentes"
CATEG_BASE_DIR = OUTPUT_DIR / "incidentes_categorizados"  # salida de incidentes categorizados SICU

# --- Normalización de país (igual que en utils/incidentes_csv) ---
def _slugify_country(raw: str) -> str:
    """Normaliza el nombre de país (minúsculas, sin tildes, aliases simples)."""
    if not raw:
        return ""
    s = raw.strip().lower()
    s_norm = "".join(
        c for c in unicodedata.normalize("NFD", s)
        if unicodedata.category(c) != "Mn"
    )
    aliases = {
        "haiti": "haiti",
        "libia": "libia",
        "libya": "libia",
        "gaza": "gaza",
        "colombia": "colombia",
        "campello": "campello",
        "el campello": "campello",
        "mali": "mali",
        "malí": "mali",
    }
    return aliases.get(s_norm, s_norm)

def _group_by_category(items: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for it in items:
        grouped.setdefault(it["categoria_sicu"], []).append(it)
    return grouped

async def incidentes_categorizados(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    /incidentes_categorizados <pais> <YYYY-MM-DD>

    Flujo:
      1. Lee el CSV de incidentes generado por /incidentes_csv:
           output/incidentes/<pais>/incidentes_<pais>_<YYYY-MM-DD>*.csv
         (usa el archivo más reciente que coincida con ese patrón).
      2. Normaliza filas y FILTRA:
           - Solo incidentes con categoria_sicu != "Otros"
           - Y descripcion no vacía
      3. Si no hay incidentes SICU -> mensaje (sin enviar CSV/TXT vacíos).
      4. Si hay:
           - Genera CSV SICU con esos incidentes.
           - Genera TXT agrupado por categoría con descripciones completas.
           - Envía ambos al chat.
    """
    args = context.args or []
    if len(args) < 2:
        return await update.message.reply_text(
            "Uso: /incidentes_categorizados <pais> <YYYY-MM-DD>\n"
            "Ejemplo: /incidentes_categorizados libia 2025-11-15"
        )

    raw_country = args[0].strip()
    country_slug = _slugify_country(raw_country)
    day = args[1].strip()

    await update.message.reply_text(f"⏳ Procesando incidentes de {raw_country.upper()} en {day}…")

    # 1) Localizar CSV de entrada (salida de /incidentes_csv)
    in_dir = INCIDENTS_DIR / country_slug
    pattern = f"incidentes_{country_slug}_{day}*.csv"
    try:
        csv_candidates = sorted(
            in_dir.glob(pattern),
            key=lambda p: p.stat().st_mtime,
            reverse=True,
        )
    except FileNotFoundError:
        csv_candidates = []

    if not csv_candidates:
        return await update.message.reply_text(
            "❌ No encontré ningún CSV de incidentes para este país y día.\n"
            f"Carpeta: {in_dir}\n"
            f"Patrón buscado: {pattern}\n"
            "Por favor, ejecuta primero /incidentes_csv para ese país y fecha."
        )

    # Elegir el archivo más reciente
    in_csv = csv_candidates[0]

    # 2) Leer incidentes del CSV de entrada
    try:
        with in_csv.open(newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            base_rows = list(reader)
    except Exception as e:
        return await update.message.reply_text(f"❌ Error leyendo CSV de entrada {in_csv.name}: {e!r}")

    if not base_rows:
        return await update.message.reply_text("ℹ️ El CSV de incidentes está vacío. No hay eventos para clasificar.")

    # 3) Normalizar filas a un esquema común para SICU
    normalizados: List[Dict[str, Any]] = []
    for r in base_rows:
        fecha = r.get("fecha") or r.get("Fecha") or day
        hora = r.get("hora") or r.get("Hora") or ""
        pais = r.get("pais") or r.get("Pais") or r.get("PAIS") or country_slug.capitalize()
        categoria_sicu = r.get("categoria_sicu") or r.get("Categoría SICU") or "Otros"
        descripcion = (r.get("descripcion") or r.get("Breve descripción") or "").strip()
        localizacion = (r.get("localizacion") or r.get("Localización") or "").strip()
        lat = (r.get("lat") or r.get("Lat") or "").strip()
        lon = (r.get("lon") or r.get("Lon") or "").strip()
        fuente = (r.get("fuente") or r.get("Fuente_URL") or "").strip()

        normalizados.append({
            "fecha": fecha,
            "hora": hora,
            "pais": pais,
            "categoria_sicu": categoria_sicu,
            "descripcion": descripcion,
            "localizacion": localizacion,
            "lat": lat,
            "lon": lon,
            "fuente_URL": fuente,
        })

    # 4) FILTRAR solo eventos SICU "reales": categoria_sicu != "Otros" y descripción NO vacía
    filtrados: List[Dict[str, Any]] = []
    for row in normalizados:
        cat = (row.get("categoria_sicu") or "").strip().lower()
        desc = (row.get("descripcion") or "").strip()
        if not cat or not desc:
            continue
        if cat == "otros":
            continue
        filtrados.append(row)

    if not filtrados:
        return await update.message.reply_text(
            "ℹ️ No hay incidentes SICU categorizados para este país y día "
            "(solo se encontraron eventos 'Otros' o sin descripción relevante)."
        )

    # (Opcional) ordenar por fecha/hora
    filtrados.sort(key=lambda r: (r.get("fecha", ""), r.get("hora", "")))

    # 5) Guardar CSV y TXT resumen en output/incidentes_categorizados/<pais>/
    country_dir = CATEG_BASE_DIR / country_slug
    country_dir.mkdir(parents=True, exist_ok=True)

    csv_out = country_dir / f"{country_slug}-{day}_incidentes_SICU.csv"
    txt_out = country_dir / f"{country_slug}-{day}_incidentes_SICU.txt"

    # CSV SICU (solo eventos filtrados)
    try:
        with csv_out.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(
                f,
                fieldnames=["fecha", "hora", "pais", "categoria_sicu",
                            "descripcion", "localizacion", "lat", "lon", "fuente_URL"],
            )
            writer.writeheader()
            writer.writerows(filtrados)
    except Exception as e:
        return await update.message.reply_text(f"❌ Error guardando CSV SICU: {e!r}")

    # TXT agrupado por categoría (solo categorías con eventos, EXCLUYE 'Otros')
    try:
        grouped = _group_by_category(filtrados)
        lines: List[str] = []
        lines.append("Sucesos / Incidentes (Clasificación SICU)\n")
        for cat in ("Conflicto Armado", "Terrorismo", "Criminalidad",
                    "Disturbios Civiles", "Hazards"):
            items = grouped.get(cat, [])
            if not items:
                continue
            lines.append(f"{cat}:")
            for it in items:
                desc = it["descripcion"]
                loc = it["localizacion"] or "Localización no especificada"
                lines.append(f" - {desc} → {loc}")
            lines.append("")  # línea en blanco
        txt_out.write_text("\n".join(lines), encoding="utf-8")
    except Exception as e:
        return await update.message.reply_text(f"⚠️ CSV creado, pero falló el TXT: {e!r}")

    # 6) Enviar ambos archivos al chat
    try:
        await _send_document_with_retry(
            update, csv_out, f"✅ CSV SICU generado: {csv_out.name}"
        )
        await _send_document_with_retry(
            update, txt_out, f"✅ TXT resumen SICU: {txt_out.name}"
        )
    except Exception as e:
        return await update.message.reply_text(
            f"✅ Archivos creados en {country_dir}.\n⚠️ No se pudieron enviar: {e!r}"
        )

async def _send_document_with_retry(update: Update, file_path: Path, caption: str, retries: int = 2) -> None:
    attempts = 0
    while True:
        try:
            with file_path.open("rb") as fh:
                await update.message.reply_document(
                    document=InputFile(fh, filename=file_path.name),
                    caption=caption,
                )
            return
        except RetryAfter as e:
            attempts += 1
            if attempts > retries:
                raise
            await asyncio.sleep(e.retry_after + 1)