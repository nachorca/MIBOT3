# botapp/handlers/generate_report.py
# -*- coding: utf-8 -*-
from __future__ import annotations
import csv
from pathlib import Path
from typing import List, Dict, Any
from telegram import Update, InputFile
from telegram.ext import ContextTypes

from botapp.config import get_settings
# Use absolute import; if it fails, fall back to a stub implementation.
try:
    from botapp.services.llm_client import get_client  # type: ignore[import]  # absolute import
except Exception:
    # Minimal stub so static analysis / imports don't fail; if used at runtime it raises a clear error.
    class _DummyCompletions:
        @staticmethod
        def create(*args, **kwargs):
            raise RuntimeError(
                "LLM client is not configured: install and configure botapp.services.llm_client "
                "or provide a working get_client() that returns an object with chat.completions.create(...)."
            )

    class _DummyChat:
        completions = _DummyCompletions()

    class _DummyClient:
        chat = _DummyChat()

    def get_client():
        return _DummyClient()

SET = get_settings()
PROJECT_ROOT = Path(SET.data_dir).resolve().parent
OUTPUT_DIR = PROJECT_ROOT / "output"

async def generate_report_step1(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    args = context.args or []
    if len(args) < 2:
        return await update.message.reply_text(
            "Uso: /generate_report <pais> <YYYY-MM-DD>\n"
            "Ejemplo: /generate_report gaza 2025-11-23"
        )

    raw_country = args[0].strip().lower()
    day = args[1].strip()
    chat_id = update.effective_chat.id

    await update.message.reply_text(f"⏳ Cargando archivos para {raw_country.upper()} en {day}…")

    try:
        # Ruta al TXT original
        txt_path = PROJECT_ROOT / "data" / raw_country / f"{day}.txt"
        txt_original = txt_path.read_text(encoding="utf-8", errors="ignore")

        # Ruta al CSV incidentes
        csv_inc_path = OUTPUT_DIR / "incidentes" / raw_country / f"incidentes_{raw_country}_{day}.csv"
        with csv_inc_path.open(newline="", encoding="utf-8") as f:
            csv_incidentes = list(csv.DictReader(f))

        # Ruta al TXT SICU
        txt_sicu_path = OUTPUT_DIR / "incidentes_categorizados" / raw_country / f"{raw_country}-{day}_incidentes_SICU.txt"
        txt_sicu = txt_sicu_path.read_text(encoding="utf-8", errors="ignore")

        # Ruta al CSV SICU
        csv_sicu_path = OUTPUT_DIR / "incidentes_categorizados" / raw_country / f"{raw_country}-{day}_incidentes_SICU.csv"
        with csv_sicu_path.open(newline="", encoding="utf-8") as f:
            csv_sicu = list(csv.DictReader(f))

    except FileNotFoundError as e:
        return await update.message.reply_text(f"❌ Archivo no encontrado: {e}")
    except Exception as e:
        return await update.message.reply_text(f"❌ Error cargando archivos: {e!r}")

    await update.message.reply_text(
        "✅ Paso 1 completado: archivos cargados correctamente.\n"
        "Siguiente: preparación del prompt."
    )

    # Para depuración (opcional)
    # await update.message.reply_document(document=InputFile(txt_path, filename=txt_path.name))
    # await update.message.reply_document(document=InputFile(csv_inc_path, filename=csv_inc_path.name))
    # await update.message.reply_document(document=InputFile(txt_sicu_path, filename=txt_sicu_path.name))
    # await update.message.reply_document(document=InputFile(csv_sicu_path, filename=csv_sicu_path.name))

    # Almacenar datos para el siguiente paso
    context.chat_data["step1_data"] = {
        "txt_original": txt_original,
        "csv_incidentes": csv_incidentes,
        "txt_sicu": txt_sicu,
        "csv_sicu": csv_sicu,
        "country": raw_country,
        "day": day,
    }

async def generate_report_step2(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    data = context.chat_data.get("step1_data")
    if not data:
        return await update.message.reply_text(
            "❌ No se encontraron datos del paso 1. Ejecuta primero /generate_report <pais> <fecha>"
        )

    country = data["country"]
    day = data["day"]
    txt_original = data["txt_original"]
    csv_incidentes = data["csv_incidentes"]
    txt_sicu = data["txt_sicu"]
    csv_sicu = data["csv_sicu"]

    await update.message.reply_text(f"⏳ Preparando prompt para {country.upper()} {day}…")

    # Construcción de snippets para el prompt
    snippet_txt_orig = txt_original[:1000] + ("…" if len(txt_original) > 1000 else "")
    snippet_csv_inc = str(csv_incidentes[:5]) + ("…" if len(csv_incidentes) > 5 else "")
    snippet_txt_sicu = txt_sicu[:1000] + ("…" if len(txt_sicu) > 1000 else "")
    snippet_csv_sicu = str(csv_sicu[:5]) + ("…" if len(csv_sicu) > 5 else "")

    prompt = f"""
Eres un analista de seguridad de la unidad SICU.
Zona: {country.upper()}
Día operativo: {day}

Archivo-1 (texto original):\n{snippet_txt_orig}
\nArchivo-2 (CSV incidentes):\n{snippet_csv_inc}
\nArchivo-3 (TXT SICU):\n{snippet_txt_sicu}
\nArchivo-4 (CSV SICU):\n{snippet_csv_sicu}

Tu tarea:
- Analiza los eventos contenidos en los archivos.
- Genera un informe estructurado con las secciones:
  0. Encabezado
  1. Resumen Ejecutivo
  2. Desglose de Eventos
  3. Mapa de Focos y Proyección
  4. Aviación, Movilidad y Cambio
  5. Situación ONU / Autoridades
  6. Recomendaciones
- Usa la información de los archivos para aportar cifras, localizaciones, fechas/horas y tendencias.
- Si algún dato está ausente, señala claramente que no hay información disponible.
- No inventes hechos que no estén en los datos.

Salida esperada: texto listo para guardar como informe SICU.
"""

    client = get_client()
    resp = client.chat.completions.create(
        model="gpt-5.1",
        messages=[
            {"role": "system", "content": "Eres un analista de seguridad generando un informe operacional."},
            {"role": "user", "content": prompt}
        ],
        temperature=0.2
    )
    report_text = resp.choices[0].message.content

    # Guardar el informe
    report_dir = OUTPUT_DIR / "sicu_reports" / country
    report_dir.mkdir(parents=True, exist_ok=True)
    report_path = report_dir / f"{country}-{day}_SICU_REPORT.txt"
    report_path.write_text(report_text, encoding="utf-8")

    # Enviar en Telegram
    with report_path.open("rb") as f:
        await update.message.reply_document(
            document=InputFile(f, filename=report_path.name),
            caption=f"📄 INFORME SICU :: {country.upper()} {day}"
        )

    await update.message.reply_text("✅ Paso 2 completado: Informe generado.")