import os
from typing import Dict
from openai import OpenAI

_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")

def summarize_article_es(title: str, content: str) -> str:
    """
    Devuelve un resumen en español (5-8 líneas) destacando lo operativo y los hechos clave.
    """
    client = OpenAI()
    prompt = (
        "Resume en español (5-8 líneas) el siguiente artículo. "
        "Da prioridad a hechos verificables, cifras, fechas y posibles impactos locales. "
        "Evita opiniones. Devuelve texto plano sin viñetas.\n\n"
        f"TÍTULO: {title}\n\n"
        f"CONTENIDO:\n{content[:4000]}"
    )
    resp = client.responses.create(
        model=_MODEL,
        input=prompt,
        temperature=0.3,
    )
    return (resp.output_text or "").strip()