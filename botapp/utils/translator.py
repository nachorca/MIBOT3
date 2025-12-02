# -*- coding: utf-8 -*-
"""
Utilidades de traducción usando HuggingFace (MarianMT) y Argos Translate.

- Traducción principal a ESPAÑOL mediante modelos HF (opus-mt-*-es),
  con soporte para árabe, inglés, francés, criollo haitiano, ruso, hebreo/Israel y más.
- Fallback a Argos Translate cuando no hay modelo HF disponible.
- Traducción opcional a INGLÉS (translate_to_en) mediante Argos.

Se espera que los modelos `.argosmodel` estén disponibles en la carpeta indicada por
la variable de entorno `ARGOS_MODELS_DIR` o, en su defecto, en `./data/argos_models`.
"""

from __future__ import annotations

import os
import re
import json
from functools import lru_cache
from pathlib import Path
from typing import Dict, List, Optional

from contextlib import nullcontext

# Lazy import de torch para evitar cargarlo en el arranque
_torch = None
def _lazy_torch():
    """
    Carga torch bajo demanda. Devuelve:
      - el módulo torch, si pudo importarse
      - False, si no está disponible
    """
    global _torch
    if _torch is None:
        try:
            import importlib
            _torch = importlib.import_module("torch")
        except Exception:
            _torch = False
    return _torch


BASE_DATA = Path(os.getenv("DATA_DIR", "./data"))
_ARGOS_RUNTIME = Path(os.getenv("ARGOS_RUNTIME_DIR", BASE_DATA / "argos_runtime"))
_ARGOS_RUNTIME.mkdir(parents=True, exist_ok=True)
_ARGOS_CACHE = _ARGOS_RUNTIME / "cache"
_ARGOS_CACHE.mkdir(parents=True, exist_ok=True)
_ARGOS_DATA_HOME = _ARGOS_RUNTIME / "data"
_ARGOS_DATA_HOME.mkdir(parents=True, exist_ok=True)
_ARGOS_PACKAGES = _ARGOS_RUNTIME / "packages"
_ARGOS_PACKAGES.mkdir(parents=True, exist_ok=True)
_STANZA_RESOURCES = _ARGOS_RUNTIME / "stanza_resources"
_STANZA_RESOURCES.mkdir(parents=True, exist_ok=True)

# Forzar a Argos Translate a usar rutas dentro del workspace (evita permisos denegados).
os.environ.setdefault("XDG_CACHE_HOME", str(_ARGOS_CACHE))
os.environ.setdefault("XDG_DATA_HOME", str(_ARGOS_DATA_HOME))
os.environ.setdefault("ARGOS_PACKAGES_DIR", str(_ARGOS_PACKAGES))
os.environ.setdefault("ARGOS_CHUNK_TYPE", "SPACY")
os.environ.setdefault("STANZA_RESOURCES_DIR", str(_STANZA_RESOURCES))
HF_CACHE_DIR = BASE_DATA / "hf_models"
HF_CACHE_DIR.mkdir(parents=True, exist_ok=True)
os.environ.setdefault("HF_HOME", str(HF_CACHE_DIR))

_HF_REMOTE_DOWNLOADS = os.getenv("HF_ALLOW_REMOTE_MODELS", "").lower() in {
    "1",
    "true",
    "yes",
    "on",
}
if not _HF_REMOTE_DOWNLOADS:
    # Obliga a transformers/huggingface-hub a trabajar completamente offline salvo
    # que el operador defina HF_ALLOW_REMOTE_MODELS=1 explícitamente.
    os.environ.setdefault("HF_HUB_OFFLINE", "1")
    os.environ.setdefault("TRANSFORMERS_OFFLINE", "1")

# --- CACHÉ GLOBAL DE TRADUCCIONES A ESPAÑOL ---
ES_CACHE_PATH = BASE_DATA / "cache_translations_es.json"

try:
    _ES_CACHE: Dict[str, str] = json.loads(ES_CACHE_PATH.read_text(encoding="utf-8"))
except Exception:
    _ES_CACHE = {}


def _save_es_cache() -> None:
    """
    Guarda la caché de traducciones a español en disco.
    Se llama cada vez que se añade una entrada nueva.
    """
    try:
        ES_CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
        ES_CACHE_PATH.write_text(
            json.dumps(_ES_CACHE, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
    except Exception as e:
        print(f"[translator] Error guardando caché ES: {e!r}")


try:
    import langid  # type: ignore
except Exception:  # pragma: no cover - dependencia opcional
    langid = None

try:
    from argostranslate import package, translate  # type: ignore
except Exception:  # pragma: no cover - dependencia opcional
    package = None
    translate = None

try:
    from transformers import MarianMTModel, MarianTokenizer  # type: ignore
except Exception:  # pragma: no cover - dependencia opcional
    MarianMTModel = None
    MarianTokenizer = None

MODELS_DIR = Path(
    os.getenv("ARGOS_MODELS_DIR", Path(os.getenv("DATA_DIR", "./data")) / "argos_models")
)
_WS_RE = re.compile(r"\s+")

# Modelos específicos HF por idioma origen → ES
HF_MODEL_OVERRIDES = {
    "en": "Helsinki-NLP/opus-mt-en-es",
    "fr": "Helsinki-NLP/opus-mt-fr-es",
    "ar": "Helsinki-NLP/opus-mt-ar-es",
    "ru": "Helsinki-NLP/opus-mt-ru-es",  # ruso → español
    "he": "Helsinki-NLP/opus-mt-he-es",  # hebreo → español
    "iw": "Helsinki-NLP/opus-mt-he-es",  # código antiguo de hebreo
    "ht": "Helsinki-NLP/opus-mt-mul-es", # criollo haitiano via modelo multilingüe
}
# Modelo multilingüe de fallback: soporta muchos idiomas → ES
HF_MODEL_FALLBACK = "Helsinki-NLP/opus-mt-mul-es"

_ARABIC_CHARS_RE = re.compile(r"[\u0600-\u06FF]")
_HEBREW_CHARS_RE = re.compile(r"[\u0590-\u05FF]")
_CYRILLIC_CHARS_RE = re.compile(r"[\u0400-\u04FF]")

_EN_HINT_RE = re.compile(
    r"\b(?:the|and|with|from|towards|north|south|east|west|forces|police|military|government|"
    r"attack|strikes|troops|killed|injured|people|city|state|report|breaking|urgent)\b",
    re.IGNORECASE,
)
_FR_HINT_RE = re.compile(
    r"\b(?:le|la|les|des|une|un|avec|pour|selon|contre|bombardement|gouvernement|attaque|ville)\b",
    re.IGNORECASE,
)
_SPANISH_HINT_RE = re.compile(
    r"\b(?:segun|según|gobierno|personas|heridos|muertos|fuerzas|ataque|ciudad|provincia|"
    r"ultimo|minutos|urgente)\b",
    re.IGNORECASE,
)
# Algunas palabras clave típicas de criollo haitiano (heurísticas)
_HAITIAN_HINT_RE = re.compile(
    r"\b(?:ayiti|pòtoprens|okap|gonayiv|kreyo|kreyòl|pèp|lavi|lanmò)\b",
    re.IGNORECASE,
)

_SPANISH_CHARS = set("áíóúüñÁÍÓÚÜÑ")


def _looks_spanish(text: str) -> bool:
    if any(ch in _SPANISH_CHARS for ch in text):
        return True
    lowered = text.lower()
    return bool(_SPANISH_HINT_RE.search(lowered))


def _guess_language_candidates(text: str) -> List[str]:
    """
    Devuelve una lista ordenada de códigos de idioma candidatos (ej. ['ar','fr','en']).
    Usa langid si está disponible + heurísticas para árabe, francés, inglés, criollo,
    ruso (cirílico) y hebreo.
    """
    detected = _detect_language(text)
    candidates: List[str] = []

    if detected:
        candidates.append(detected)

    # Heurísticas por escritura
    if _ARABIC_CHARS_RE.search(text):
        if "ar" not in candidates:
            candidates.append("ar")
    if _HEBREW_CHARS_RE.search(text):
        if "he" not in candidates:
            candidates.append("he")
    if _CYRILLIC_CHARS_RE.search(text):
        if "ru" not in candidates:
            candidates.append("ru")

    lowered = text.lower()
    if _FR_HINT_RE.search(lowered) and "fr" not in candidates:
        candidates.append("fr")
    if _EN_HINT_RE.search(lowered) and "en" not in candidates:
        candidates.append("en")
    if _HAITIAN_HINT_RE.search(lowered) and "ht" not in candidates:
        candidates.append("ht")

    # Si no hay candidatos claros, intentar decidir si parece inglés por ASCII
    letters = [ch for ch in text if ch.isalpha()]
    ascii_letters = [ch for ch in letters if ch.isascii()]
    ascii_ratio = (len(ascii_letters) / len(letters)) if letters else 0.0
    if (
        not candidates
        and ascii_ratio > 0.85
        and not _looks_spanish(text)
        and any(ch.isascii() for ch in text)
    ):
        candidates.append("en")

    return candidates or ([] if not detected else [detected])


@lru_cache(maxsize=1)
def _load_installed_languages() -> Dict[str, object]:
    """
    Carga los modelos de Argos disponibles en MODELS_DIR (si existen) y
    devuelve un diccionario {codigo_idioma: Language}.
    """
    if package is None or translate is None:
        return {}
    if MODELS_DIR.exists():
        for pkg in sorted(MODELS_DIR.glob("*.argosmodel")):
            try:
                package.install_from_path(str(pkg))
            except Exception:
                # Si ya está instalado o el paquete está dañado, continuamos con el resto.
                continue

    langs = {}
    try:
        for lang in translate.get_installed_languages():
            langs[getattr(lang, "code", "")] = lang
    except Exception:
        return {}
    return langs


def _normalize(text: str) -> str:
    return _WS_RE.sub(" ", text).strip()


def _shorten(text: str, max_chars: int) -> str:
    if len(text) <= max_chars:
        return text

    for splitter in (". ", "! ", "? "):
        idx = text.find(splitter)
        if 0 < idx < max_chars:
            return text[: idx + 1]

    return text[: max_chars - 3].rstrip() + "..."


def _detect_language(text: str) -> Optional[str]:
    if langid is None:
        return None
    try:
        code, _ = langid.classify(text)
        return code
    except Exception:
        return None


@lru_cache(maxsize=8)
def _load_hf_model(model_name: str):
    # Evita construir el modelo si torch no está disponible
    torch = _lazy_torch()
    if torch is False:
        # Forzamos a que el llamador haga fallback (Argos) sin romper
        raise RuntimeError("PyTorch no disponible para cargar modelo HF")

    if MarianTokenizer is None or MarianMTModel is None:
        raise RuntimeError("Transformers no disponible")

    try:
        tokenizer = MarianTokenizer.from_pretrained(
            model_name,
            cache_dir=str(HF_CACHE_DIR),
            local_files_only=True,
        )
        model = MarianMTModel.from_pretrained(
            model_name,
            cache_dir=str(HF_CACHE_DIR),
            local_files_only=True,
        )
    except Exception:
        if not _HF_REMOTE_DOWNLOADS:
            raise
        tokenizer = MarianTokenizer.from_pretrained(
            model_name,
            cache_dir=str(HF_CACHE_DIR),
            local_files_only=False,
        )
        model = MarianMTModel.from_pretrained(
            model_name,
            cache_dir=str(HF_CACHE_DIR),
            local_files_only=False,
        )

    # Estas llamadas usan torch; ahora son seguras porque ya comprobamos arriba
    model.eval()
    model.to("cpu")
    return tokenizer, model


def _hf_translate(text: str, lang_code: str) -> str:
    if not text or MarianTokenizer is None or MarianMTModel is None:
        return ""

    model_name = HF_MODEL_OVERRIDES.get(lang_code, HF_MODEL_FALLBACK)
    try:
        tokenizer, model = _load_hf_model(model_name)
    except Exception:
        if model_name == HF_MODEL_FALLBACK:
            return ""
        try:
            tokenizer, model = _load_hf_model(HF_MODEL_FALLBACK)
        except Exception:
            return ""

    # Asegúrate de tener torch ANTES de crear tensores "pt"
    torch = _lazy_torch()
    if torch is False:
        return ""

    try:
        batch = tokenizer(
            [text],
            return_tensors="pt",
            padding=True,
            truncation=True,
            max_length=512,
        )

        ctx = torch.no_grad() if hasattr(torch, "no_grad") else nullcontext()
        with ctx:
            generated = model.generate(
                **batch,
                max_length=512,
                num_beams=4,
                early_stopping=True,
            )

        decoded = tokenizer.batch_decode(generated, skip_special_tokens=True)
        return decoded[0] if decoded else ""
    except Exception:
        return ""


def _attempt_translation(cleaned: str, lang_code: str) -> str:
    translated = _hf_translate(cleaned, lang_code)
    if translated:
        return _normalize(translated)

    if translate is None:
        return ""

    languages = _load_installed_languages()
    src = languages.get(lang_code)
    tgt = languages.get("es")
    if not src or not tgt:
        return ""
    try:
        output = src.get_translation(tgt).translate(cleaned)
    except Exception:
        return ""
    return _normalize(output) or ""


def _translate_to_spanish(text: str) -> str:
    """
    Lógica central de traducción a español:
    - Normaliza el texto.
    - Detecta idioma (langid + heurísticas).
    - Usa modelos HF si están disponibles, con fallback a Argos.
    """
    cleaned = _normalize(text or "")
    if not cleaned:
        return ""

    # Detectar candidatos de idioma
    candidates = _guess_language_candidates(cleaned)

    # Si parece ya español, devolver tal cual
    if _looks_spanish(cleaned) or ("es" in candidates and candidates[0] == "es"):
        return cleaned

    # Probar traducción con cada candidato
    for lang_code in candidates:
        if lang_code == "es":
            return cleaned
        translated = _attempt_translation(cleaned, lang_code)
        if translated:
            return translated

    # Si no se pudo traducir, devolver el original
    return cleaned


def translate_to_es(text: str, max_chars: int = 0) -> str:
    """
    Traduce un texto al español (si hay modelos disponibles) usando caché en disco.
    max_chars > 0 recorta el resultado al número de caracteres indicado.
    """
    cleaned = (text or "").strip()
    if not cleaned:
        return ""

    key = f"{max_chars}|{cleaned}"
    if key in _ES_CACHE:
        return _ES_CACHE[key]

    translated = _translate_to_spanish(cleaned)
    if not translated:
        out = ""
    else:
        out = translated
        if max_chars > 0:
            out = _shorten(out, max_chars)

    _ES_CACHE[key] = out
    _save_es_cache()
    return out


def to_spanish_excerpt(text: str, max_chars: int = 400) -> str:
    """
    Devuelve una versión en español (resumida) del texto original.
    - Normaliza espacios.
    - Detecta idioma y, si no es español y hay modelo disponible, traduce.
    - Recorta la salida a `max_chars` respetando puntos finales cuando sea posible.
    """
    translated = _translate_to_spanish(text)
    if not translated:
        return ""
    return _shorten(translated, max_chars)


def to_spanish_full(text: str) -> str:
    """
    Traduce un bloque completo de texto al ESPAÑOL sin recortarlo ni resumirlo.
    Ahora reutiliza translate_to_es, compartiendo la caché de traducciones.
    """
    return translate_to_es(text, max_chars=0)


# ====================== NUEVO: traducción a INGLÉS ======================

def _argos_translate_to_en(cleaned: str, src_code: Optional[str]) -> str:
    """
    Usa Argos Translate para traducir cleaned -> EN, si hay modelos instalados.
    - src_code: código ISO de idioma detectado (ej. 'ar','fr','es','en') o None/'auto'.
    """
    if package is None or translate is None:
        return ""

    languages = _load_installed_languages()
    if not languages:
        return ""

    tgt = languages.get("en")
    if not tgt:
        # No hay modelo con destino 'en'
        return ""

    # Si tenemos código de origen concreto y está instalado
    if src_code and src_code in languages and src_code != "en":
        src = languages[src_code]
        try:
            out = src.get_translation(tgt).translate(cleaned)
            if out:
                return _normalize(out)
        except Exception:
            pass

    # Sin código fiable: probar con cualquier idioma que tenga traducción a 'en'
    for code, src in languages.items():
        if code == "en":
            continue
        try:
            out = src.get_translation(tgt).translate(cleaned)
            if out:
                return _normalize(out)
        except Exception:
            continue

    return ""


def translate_to_en(text: str, max_chars: int = 0) -> str:
    """
    Traduce cualquier texto a INGLÉS usando:
      - Detección de idioma (langid + heurísticas árabe/inglés/francés/criollo/ruso/hebreo).
      - Argos Translate para src -> en (por ejemplo, ar->en).
    Si no hay modelos disponibles o falla, devuelve el texto original.
    max_chars > 0 recorta la salida.
    """
    cleaned = _normalize(text)
    if not cleaned:
        return ""

    # Si parece ya inglés, lo dejamos
    if _EN_HINT_RE.search(cleaned) and not _ARABIC_CHARS_RE.search(cleaned):
        out = cleaned
    else:
        lang = _detect_language(cleaned)
        # Si lang es 'en', devolver sin tocar
        if lang == "en":
            out = cleaned
        else:
            # Si vemos caracteres árabes, forzar 'ar'
            if _ARABIC_CHARS_RE.search(cleaned):
                lang = "ar"
            # Intentar con Argos
            out = _argos_translate_to_en(cleaned, lang)

            # Si no ha ido bien y el texto parece español, podemos intentar es->en
            if not out and _looks_spanish(cleaned):
                out = _argos_translate_to_en(cleaned, "es")

            # Como último fallback, si sigue vacío, devolvemos original
            if not out:
                out = cleaned

    if max_chars > 0:
        return _shorten(out, max_chars)
    return out