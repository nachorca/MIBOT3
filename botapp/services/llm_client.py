# botapp/services/llm_client.py
from __future__ import annotations

import os
import json
from typing import List, Dict
import asyncio

try:
    from openai import OpenAI
except ImportError:
    OpenAI = None  # Se controlará en tiempo de ejecución


def _build_sicu_prompt(country: str, day: str, incidents: List[Dict[str, str]]) -> str:
    """
    Construye un prompt textual compacto con la información principal del CSV SICU.
    """
    by_cat: Dict[str, List[Dict[str, str]]] = {}
    for row in incidents:
        cat = (row.get("categoria_sicu") or "Sin categoría").strip()
        by_cat.setdefault(cat, []).append(row)

    lines = []
    lines.append(f"País/Área SRM: {country}")
    lines.append(f"Día operativo: {day}")
    lines.append("")
    lines.append("RESUMEN DE INCIDENTES POR CATEGORÍA (datos brutos para tu razonamiento):")

    for cat, rows in by_cat.items():
        lines.append(f"\n=== {cat.upper()} ({len(rows)} incidentes) ===")
        for r in rows[:30]:  # limita para no pasarse de tokens
            loc = r.get("localizacion") or "Localización no especificada"
            fh = f"{r.get('fecha','')} {r.get('hora','')}".strip()
            desc = (r.get("descripcion") or "").strip().replace("\n", " ")
            fuente = (r.get("fuente_URL") or "")[:200]
            lines.append(f"- [{fh}] {loc}: {desc}")
            if fuente:
                lines.append(f"  Fuente: {fuente}")

    return "\n".join(lines)


async def generate_sicu_analysis(
    country: str,
    day: str,
    incidents: List[Dict[str, str]],
    model: str = "gpt-4.1-mini",
) -> str:
    """
    Llama a la API de OpenAI para generar un análisis de situación SICU
    a partir de la lista de incidentes (ya deduplicados).
    Devuelve un texto listo para pegar como secciones 1, 3, 5, 6 del informe.
    """
    if OpenAI is None:
        return (
            "❌ No se encontró la librería 'openai'. Instálala en el entorno actual:\n"
            "    python -m pip install openai\n"
        )

    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        return (
            "❌ No hay OPENAI_API_KEY en el entorno.\n"
            "Añade a tu .env o exporta en la shell, por ejemplo:\n"
            "    export OPENAI_API_KEY='sk-...'\n"
        )

    client = OpenAI(api_key=api_key)

    system_message = (
        "Eres un analista de seguridad de Naciones Unidas especializado en SRM/SICU. "
        "Vas a recibir un resumen de incidentes ya clasificados por categoría para un día operativo. "
        "Tu tarea es redactar un INFORME ANALÍTICO en ESPAÑOL, siguiendo esta estructura:\n\n"
        "1. RESUMEN EJECUTIVO (4–7 puntos numerados, muy sintéticos y operativos).\n"
        "3. MAPA DE FOCOS Y TENDENCIAS (por zonas/ciudades, actores, evolución, riesgos clave).\n"
        "5. SITUACIÓN MISIÓN ONU / AUTORIDADES / FUERZA MULTINACIONAL (indica si hay cambios, restricciones de movimiento, amenazas específicas, narrativa pública, etc.).\n"
        "6. RECOMENDACIONES OPERATIVAS (3–7 recomendaciones concretas para seguridad, movilidad y protección del personal ONU/INGOs).\n\n"
        "No repitas toda la lista de incidentes: sintetiza y prioriza amenazas, riesgos y recomendaciones."
    )

    user_message = (
        f"País/Área: {country}, día operativo {day}.\n\n"
        "A continuación tienes los incidentes SICU (ya agregados y deduplicados). "
        "Úsalos como base para tu análisis:\n\n"
        f"{_build_sicu_prompt(country, day, incidents)}"
    )

    def _call_api() -> str:
        # Usamos la API de respuestas (OpenAI SDK 1.x)
        resp = client.responses.create(
            model=model,
            input=[
                {"role": "system", "content": system_message},
                {"role": "user", "content": user_message},
            ],
            max_output_tokens=1200,
        )
        # Extraer el texto principal (primer bloque)
        # Ver docs de OpenAI Python 1.x para Responses API
        chunks = []
        for item in resp.output:
            for content_part in getattr(item, "content", []):
                if getattr(content_part, "type", None) == "output_text":
                    chunks.append(content_part.text)
        if not chunks:
            # fallback por si el formato cambia
            try:
                return json.dumps(resp.to_dict(), ensure_ascii=False)
            except Exception:
                return "⚠️ No se pudo extraer el texto del modelo."
        return "\n".join(chunks).strip()

    try:
        text = await asyncio.to_thread(_call_api)
    except Exception as e:
        return f"❌ Error llamando a la API de OpenAI: {e}"

    return text