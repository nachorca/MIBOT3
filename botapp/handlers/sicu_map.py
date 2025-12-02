# botapp/handlers/sicu_map.py
from __future__ import annotations
from pathlib import Path
from datetime import datetime, timezone, timedelta
from telegram import Update, InputFile
from telegram.ext import ContextTypes, CommandHandler
from services.sicu_map import build_sicu_map
import shutil

# Zona horaria (Trípoli)
TZ_TRIPOLI = timezone(timedelta(hours=2))

# Rutas fijas
OUTPUT_DIR = Path("/Users/joseignaciosantiagomartin/curso de python/MIBOT3/output")
INCIDENTS_DIR = OUTPUT_DIR / "incidentes"  # carpeta con CSV de incidentes

def _today_tripoli_iso() -> str:
    """YYYY-MM-DD en zona Trípoli."""
    return datetime.now(TZ_TRIPOLI).date().isoformat()

def _paths(country: str, date_str: str | None):
    """
    Devuelve rutas normalizadas:
      CSV estándar : output/incidentes/incidentes_<pais>_<fecha>.csv
      HTML salida  : output/mapa_eventos_sicu_<pais>_<fecha>.html
    """
    country = (country or "").strip().lower()
    date_used = (date_str or _today_tripoli_iso()).strip()
    csv_std = INCIDENTS_DIR / f"incidentes_{country}_{date_used}.csv"
    html_out = OUTPUT_DIR / f"mapa_eventos_sicu_{country}_{date_used}.html"
    return csv_std, html_out, date_used, country

# === Normalización de CSV al esquema requerido por build_sicu_map ===
REQUIRED_FIELDS = [
    "Categoría SICU",
    "Breve descripción",
    "Fecha",
    "Hora",
    "Localización",
    "Subcategoría",
    "Nivel de severidad",
]

# Aliases comunes -> columna requerida
HEADER_ALIASES = {
    # categoría
    "categoria_sicu": "Categoría SICU",
    "categoría_sicu": "Categoría SICU",
    "categoria": "Categoría SICU",
    "categoría": "Categoría SICU",
    "type": "Categoría SICU",
    # descripción
    "descripcion": "Breve descripción",
    "descripción": "Breve descripción",
    "description": "Breve descripción",
    "detalle": "Breve descripción",
    # fecha
    "fecha": "Fecha",
    "date": "Fecha",
    # hora
    "hora": "Hora",
    "time": "Hora",
    # localización
    "localizacion": "Localización",
    "localización": "Localización",
    "location": "Localización",
    "lugar": "Localización",
    # subcategoría
    "subcategoria": "Subcategoría",
    "subcategoría": "Subcategoría",
    "subcategory": "Subcategoría",
    # severidad
    "nivel": "Nivel de severidad",
    "severidad": "Nivel de severidad",
    "severity": "Nivel de severidad",
}

def _normalize_category(value: str) -> str:
    c = (value or "").strip().lower()
    if any(k in c for k in ["conflicto", "armed", "combate", "enfrent", "hostilidad"]):
        return "Conflicto Armado"
    if any(k in c for k in ["terror", "ied", "vbied", "suicide", "bomba"]):
        return "Terrorismo"
    if any(k in c for k in ["disturb", "protest", "riot", "manifest", "unrest", "bloqueo"]):
        return "Disturbios Civiles"
    if any(k in c for k in ["hazard", "clima", "meteo", "inund", "incend", "accident", "desastre", "natural"]):
        return "Hazards"
    if any(k in c for k in ["crimen", "delinc", "rob", "asalto", "homic", "secuest", "extorsi", "theft", "crime"]):
        return "Criminalidad"
    return "Otros"

def _normalize_severity(value: str) -> str:
    v = (value or "").strip().lower()
    if v in {"alta", "alto", "high"}: return "Alta"
    if v in {"media", "medio", "medium"}: return "Media"
    if v in {"baja", "bajo", "low"}: return "Baja"
    return "Media"  # por defecto

def _normalize_csv_to_required(csv_in: Path, csv_out: Path, default_date: str):
    """
    Lee csv_in (UTF-8 o latin1), intenta mapear/crear REQUIRED_FIELDS y escribe csv_out (UTF-8).
    Lanza ValueError si no hay forma de componer columnas mínimas.
    """
    import csv, io

    raw = csv_in.read_bytes()
    text = None
    for enc in ("utf-8", "utf-8-sig", "latin-1"):
        try:
            text = raw.decode(enc)
            break
        except Exception:
            continue
    if text is None:
        raise ValueError("No se pudo decodificar el CSV (utf-8/latin-1)")

    # normalizamos saltos
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    # detectar delimitador
    try:
        sniff_sample = "\n".join(text.split("\n")[:50])
        dialect = csv.Sniffer().sniff(sniff_sample, delimiters=[",",";","\t","|"])
        delim = dialect.delimiter
    except Exception:
        delim = ","

    reader = csv.reader(io.StringIO(text), delimiter=delim)
    try:
        hdr = next(reader)
    except StopIteration:
        raise ValueError("CSV vacío")

    # mapa de índice -> nombre requerido (o None si desconocido)
    idx_to_req = {}
    for i, h in enumerate(hdr):
        key = (h or "").strip()
        k = key.lower()
        req = HEADER_ALIASES.get(k)
        if req:
            idx_to_req[i] = req
        elif key in REQUIRED_FIELDS:
            idx_to_req[i] = key
        else:
            idx_to_req[i] = None  # columna no usada

    # recorre filas y crea diccionario con REQUIRED_FIELDS
    rows = []
    for parts in reader:
        if not any(parts):
            continue
        row = {f: "" for f in REQUIRED_FIELDS}
        for i, val in enumerate(parts):
            req = idx_to_req.get(i)
            if not req:
                continue
            row[req] = (val or "").strip()

        # defaults / normalizaciones
        if not row["Fecha"]:
            row["Fecha"] = default_date
        row["Categoría SICU"] = _normalize_category(row["Categoría SICU"])
        row["Nivel de severidad"] = _normalize_severity(row["Nivel de severidad"])
        rows.append(row)

    # Comprobar si tenemos al menos descripción + categoría + localización
    valid = [r for r in rows if r["Breve descripción"] or r["Localización"] or r["Categoría SICU"]]
    if not valid:
        raise ValueError("No se pudieron construir filas válidas para el esquema requerido.")

    # escribir CSV normalizado
    with open(csv_out, "w", newline="", encoding="utf-8") as f:
        w = import_csv_writer = __import__("csv").DictWriter  # evitar sombrear nombre
        w = import_csv_writer(f, fieldnames=REQUIRED_FIELDS)
        w.writeheader()
        w.writerows(valid)

    return csv_out

async def sicu_map_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    /sicu_map <pais> [YYYY-MM-DD] [ruta_csv_opcional]
    - Siempre deja un **único CSV por día** con nombre estándar: incidentes_<pais>_<fecha>.csv
      (si el CSV viene con otro nombre, lo copia/sobrescribe al estándar).
    - Siempre **reemplaza** el HTML de salida si ya existe.
    """
    try:
        args = context.args or []
        if not args:
            return await update.effective_message.reply_text(
                "Uso: /sicu_map <pais> [YYYY-MM-DD] [ruta_csv_opcional]\n"
                "Ej.: /sicu_map haiti 2025-11-11"
            )

        # Parámetros
        country_arg = args[0].strip().lower()
        date_arg = args[1].strip() if len(args) >= 2 else None
        csv_arg = args[2].strip() if len(args) >= 3 else None

        # Rutas normalizadas (estándar)
        csv_std, html_out, date_used, country = _paths(country_arg, date_arg)
        INCIDENTS_DIR.mkdir(parents=True, exist_ok=True)
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

        # 1) Resolver CSV de entrada:
        #    - Si se pasa ruta, la usamos; si no, esperamos el estándar.
        #    - Si el origen no es el estándar, copiamos/sobrescribimos al estándar (dejamos 1 archivo por día).
        if csv_arg:
            src = Path(csv_arg)
            if not src.exists():
                return await update.effective_message.reply_text(
                    f"❌ No existe el CSV proporcionado:\n{src}"
                )
            try:
                shutil.copy2(src, csv_std)  # sobrescribe si existe
            except Exception as e:
                return await update.effective_message.reply_text(
                    f"❌ No pude copiar al nombre estándar:\n{csv_std}\nDetalle: {e!r}"
                )
        else:
            if not csv_std.exists():
                lista = "\n".join(p.name for p in INCIDENTS_DIR.glob(f"incidentes_{country}_*.csv")) or "(no hay archivos)"
                return await update.effective_message.reply_text(
                    "❌ No se encontró el CSV estándar del día:\n"
                    f"{csv_std}\n\n"
                    "Archivos disponibles en la carpeta:\n"
                    f"{lista}\n\n"
                    "Sugerencia: pásame la ruta del CSV como 3er argumento para normalizarlo automáticamente."
                )

        # 2) Normalizar CSV a esquema requerido (se escribe en el mismo estándar)
        try:
            _normalize_csv_to_required(csv_std, csv_std, default_date=date_used)
        except ValueError as ve:
            return await update.effective_message.reply_text(
                "❌ CSV inválido para el mapa SICU.\n"
                f"Detalle: {ve}\n"
                "Consejo: revisa que el CSV tenga información mínima (categoría/descripcion/localización)."
            )
        except Exception as e:
            return await update.effective_message.reply_text(
                "❌ Error normalizando el CSV al esquema requerido.\n"
                f"Detalle: {type(e).__name__}: {e}"
            )

        # 3) Asegurar reemplazo del HTML: si existe, eliminarlo para escribir uno limpio
        if html_out.exists():
            try:
                html_out.unlink()
            except Exception:
                pass

        # 4) Generar mapa desde el CSV estándar ya normalizado
        out_file = build_sicu_map(str(csv_std), str(html_out))

        # 5) Enviar HTML
        with open(out_file, "rb") as f:
            await update.effective_message.reply_document(
                document=InputFile(f, filename=html_out.name),
                caption=(
                    f"🗺️ Mapa SICU • {country.upper()} • {date_used}\n"
                    f"📄 CSV del día: {csv_std.name}\n"
                    "♻️ Salida reemplazada si existía."
                ),
            )

    except Exception as e:
        await update.effective_message.reply_text(
            "⚠️ Error generando el mapa SICU.\n"
            f"Detalle: {type(e).__name__}: {e}\n"
            "Si persiste, pega aquí el traceback completo."
        )

def get_handlers():
    """Registra el comando /sicu_map en el bot."""
    return [CommandHandler("sicu_map", sicu_map_cmd)]