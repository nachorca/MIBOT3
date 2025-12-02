# -*- coding: utf-8 -*-
# botapp/handlers/sicu_full.py
from __future__ import annotations

from pathlib import Path
from typing import Dict, Any, List
from datetime import datetime
import csv
from collections import defaultdict, Counter
from difflib import SequenceMatcher  # para similitud de descripciones

from telegram import Update, InputFile
from telegram.ext import ContextTypes

from botapp.config import get_settings
from botapp.utils.incidentes_csv import save_incidentes_csv_from_txt, _slugify_country
from botapp.utils.csv_to_kml import csv_to_kml
from botapp.services.report_hooks import registrar_incidentes_desde_texto

SET = get_settings()

DATA_DIR = Path(SET.data_dir).resolve()
PROJECT_ROOT = DATA_DIR.parent if DATA_DIR.parent != DATA_DIR else Path(__file__).resolve().parents[2]
OUTPUT_DIR = PROJECT_ROOT / "output"
INCIDENTS_DIR = OUTPUT_DIR / "incidentes"
CATEG_BASE_DIR = OUTPUT_DIR / "incidentes_categorizados"

# Países para la automatización (ajusta la lista a tu gusto)
AUTO_SICU_COUNTRIES = ["libia", "haiti", "gaza", "colombia", "campello", "mali"]


def _country_dir(country: str) -> Path:
    d = DATA_DIR / country.lower()
    d.mkdir(parents=True, exist_ok=True)
    return d


def _group_by_category(items: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for it in items:
        grouped.setdefault(it["categoria_sicu"], []).append(it)
    return grouped


def _parse_time_to_minutes(hora: str) -> int | None:
    """
    Convierte 'HH:MM' a minutos desde medianoche. Devuelve None si no es válida.
    """
    hora = (hora or "").strip()
    if not hora:
        return None
    try:
        parts = hora.split(":")
        h = int(parts[0])
        m = int(parts[1]) if len(parts) > 1 else 0
        return h * 60 + m
    except Exception:
        return None


def _similarity(a: str, b: str) -> float:
    """
    Similaridad simple entre dos textos (0.0–1.0) usando difflib.
    """
    a = (a or "").strip().lower()
    b = (b or "").strip().lower()
    if not a or not b:
        return 0.0
    return SequenceMatcher(None, a, b).ratio()


def deduplicate_sicu_incidents(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Deduplica incidentes SICU combinando filas muy similares dentro de la misma clave
    (pais, categoria_sicu, fecha, localizacion):

    - Agrupa por (pais_norm, cat_norm, fecha, loc_norm).
    - Dentro de cada grupo, crea clusters:
      - Si la descripción es muy parecida (≥ 0.75) y la hora está a ±120 min
        (si ambas existen), se considera el mismo incidente.
    - Fusiona:
      - fuente_URL: concatena todas las fuentes sin duplicados (separadas por " | ").
      - lat/lon: usa la primera no vacía encontrada.
    """
    grouped: Dict[tuple[str, str, str, str], List[Dict[str, Any]]] = {}

    for r in rows:
        pais = (r.get("pais") or "").strip().lower()
        cat = (r.get("categoria_sicu") or "").strip().lower()
        fecha = (r.get("fecha") or "").strip()
        loc = (r.get("localizacion") or "").strip().lower()
        key = (pais, cat, fecha, loc)
        grouped.setdefault(key, []).append(r)

    deduped: List[Dict[str, Any]] = []

    for key, items in grouped.items():
        clusters: List[List[Dict[str, Any]]] = []

        for row in items:
            desc = (row.get("descripcion") or "").strip()
            t_min = _parse_time_to_minutes(row.get("hora") or "")
            placed = False

            for cluster in clusters:
                rep = cluster[0]
                rep_desc = (rep.get("descripcion") or "").strip()
                sim = _similarity(desc, rep_desc)
                if sim < 0.75:
                    continue

                rep_t_min = _parse_time_to_minutes(rep.get("hora") or "")
                # Si ambas horas son válidas, exigimos que estén razonablemente cerca
                if t_min is not None and rep_t_min is not None:
                    if abs(t_min - rep_t_min) > 120:  # más de 2h de diferencia
                        continue

                # Si llegamos aquí, consideramos que es el mismo incidente
                cluster.append(row)
                placed = True
                break

            if not placed:
                clusters.append([row])

        # Fusionar cada cluster en una sola fila
        for cluster in clusters:
            if len(cluster) == 1:
                deduped.append(cluster[0])
                continue

            base = dict(cluster[0])  # copiar primera como base

            # Fusionar fuentes
            fuentes: List[str] = []
            for r in cluster:
                f = (r.get("fuente_URL") or r.get("fuente") or "").strip()
                if f and f not in fuentes:
                    fuentes.append(f)
            if fuentes:
                base["fuente_URL"] = " | ".join(fuentes)

            # Fusionar lat/lon: primera no vacía
            if not (base.get("lat") or "").strip():
                for r in cluster:
                    lat = (r.get("lat") or "").strip()
                    if lat:
                        base["lat"] = lat
                        break
            if not (base.get("lon") or "").strip():
                for r in cluster:
                    lon = (r.get("lon") or "").strip()
                    if lon:
                        base["lon"] = lon
                        break

            deduped.append(base)

    return deduped


def _build_sicu_report_txt(
    raw_country: str,
    country_slug: str,
    day: str,
    filtrados: List[Dict[str, Any]],
) -> str:
    """
    Construye el INFORME SICU TXT siguiendo la plantilla definitiva.
    Usa los incidentes SICU ya filtrados (sin 'Otros').
    Esta versión *no utiliza LLM*, solo integra datos.
    """
    pais = raw_country.upper()
    area_srm = country_slug.capitalize()
    fecha_op = day
    hora_edicion = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Agrupar por categoría
    by_cat: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for r in filtrados:
        cat = (r.get("categoria_sicu") or "Otros").strip()
        by_cat[cat].append(r)

    total = len(filtrados)
    cat_order = [
        "Terrorismo",
        "Conflicto Armado",
        "Criminalidad",
        "Disturbios Civiles",
        "Hazards",
    ]

    lines: List[str] = []

    # 0. ENCABEZADO
    lines.append("🧱 INFORME SICU – VERSIÓN AUTOMÁTICA")
    lines.append("⸻")
    lines.append("0. ENCABEZADO")
    lines.append(f"\t•\tPaís / Área SRM: {pais} / {area_srm}")
    lines.append(f"\t•\tFecha (día operativo): {fecha_op}")
    lines.append(f"\t•\tHora de edición: {hora_edicion}")
    lines.append("\t•\tUnidad emisora: SANTIAGOLEGALCONSULTING – Unidad de Análisis SICU")
    lines.append("\t•\tFuentes abiertas + incidentes SICU del día")
    lines.append("")

    # METEOROLOGÍA (estructura para rellenar después)
    lines.append("⸻")
    lines.append("🌤 METEOROLOGÍA")
    lines.append("")
    lines.append("(OWM / AEMET según país)")
    lines.append("\t•\tTemp / ST: [por integrar]")
    lines.append("\t•\tViento: [por integrar]")
    lines.append("\t•\tPresión: [por integrar]")
    lines.append("\t•\tVisibilidad: [por integrar]")
    lines.append("\t•\tNubosidad: [por integrar]")
    lines.append("\t•\tProbabilidad precipitación: [por integrar]")
    lines.append("\t•\tMini-pronóstico 6–12 h: [por integrar]")
    lines.append("\t•\tImpacto operativo: [pendiente de análisis específico]")
    lines.append("")

    # 1. RESUMEN EJECUTIVO – datos básicos solamente
    lines.append("⸻")
    lines.append("1. RESUMEN EJECUTIVO")
    lines.append("")
    lines.append(f"(Día operativo {fecha_op} – total incidentes SICU: {total})")
    lines.append("")
    for cat in cat_order:
        n = len(by_cat.get(cat, []))
        if n:
            lines.append(f"• {cat}: {n} incidente(s) registrado(s).")
    lines.append("")
    lines.append("➤ Análisis cualitativo: [Por integrar manualmente]")
    lines.append("")

    # 2. DESGLOSE DE EVENTOS POR CATEGORÍAS SICU
    lines.append("⸻")
    lines.append("2. DESGLOSE DE EVENTOS POR CATEGORÍAS SICU")
    lines.append("")
    lines.append("(En cada subapartado se añade: Descripción general + incidentes con formato obligatorio)")
    lines.append("")

    def add_section(cat_name: str, titulo: str):
        items = by_cat.get(cat_name, [])
        lines.append("⸻")
        lines.append(titulo)
        lines.append("")
        if not items:
            lines.append("\tNo se registraron incidentes en esta categoría durante el día operativo.")
            lines.append("")
            return

        # Resumen automático de la categoría usando tus datos
        locs = [(it.get("localizacion") or "Localización no especificada") for it in items]
        loc_counts = Counter(locs)
        top_locs = ", ".join(f"{loc} ({n})" for loc, n in loc_counts.most_common(3))

        lines.append(f"\t• Incidentes registrados: {len(items)}")
        if top_locs:
            lines.append(f"\t• Principales áreas afectadas: {top_locs}")
        lines.append("\t• Descripción general: Ver bloque 1.")
        lines.append("")

        for it in items:
            fecha_i = it.get("fecha", "")
            hora_i = it.get("hora", "")
            loc = it.get("localizacion") or "Localización no especificada"
            desc = (it.get("descripcion") or "").strip()
            fuente = (it.get("fuente_URL") or it.get("fuente") or "").strip()
            lines.append(f"\t• Localización: {loc}")
            lines.append(f"\t\tBreve descripción analítica: {desc}")
            lines.append(f"\t\tFecha/Hora: {fecha_i} {hora_i}")
            if fuente:
                lines.append(f"\t\tFuente: {fuente}")
            lines.append("")

    add_section("Terrorismo", "2.1. TERRORISMO")
    add_section("Conflicto Armado", "2.2. CONFLICTO ARMADO")
    add_section("Criminalidad", "2.3. CRIMINALIDAD")
    add_section("Disturbios Civiles", "2.4. DISTURBIOS CIVILES")
    add_section("Hazards", "2.5. HAZARDS")

    # 3. MAPA DE FOCOS Y PROYECCIÓN
    lines.append("⸻")
    lines.append("3. MAPA DE FOCOS (24 h) Y PROYECCIÓN 24–72 h")
    lines.append("")
    lines.append("Focos de hoy (24 h):")
    for cat in cat_order:
        n = len(by_cat.get(cat, []))
        if n:
            areas = ", ".join({it.get("localizacion") or "localización no especificada"
                                for it in by_cat[cat]})
            lines.append(f"\t• {cat}: {n} foco(s) – principales áreas: {areas}")
    if not any(len(by_cat.get(cat, [])) for cat in cat_order):
        lines.append("\t• Sin focos SICU identificados en las últimas 24 h.")
    lines.append("")
    lines.append("Proyección 24–72 h: [Por integrar manualmente]")
    lines.append("")

    # 4. AVIACIÓN, MOVILIDAD Y CAMBIO
    lines.append("⸻")
    lines.append("4. AVIACIÓN, MOVILIDAD Y CAMBIO")
    lines.append("")
    lines.append("Aviación:")
    lines.append("\t• Estado de aeropuertos / helipuertos / corredores aéreos: [por integrar]")
    lines.append("\t• NOTAM relevantes: [por integrar]")
    lines.append("\t• Actividad aérea militar (UAV, artillería, jets): [por integrar]")
    lines.append("\t• Impacto meteorológico en vuelos / evacuaciones: [por integrar]")
    lines.append("")
    lines.append("Movilidad:")
    lines.append("\t• MSR activas / cerradas: [por integrar]")
    lines.append("\t• Chequeos, bloqueos, focos de violencia: [por integrar]")
    lines.append("\t• Riesgos de convoyes (UXO/MUSE, bandas, facciones armadas): [por integrar]")
    lines.append("\t• Corredores recomendados: [por integrar]")
    lines.append("\t• Zonas a restringir o prohibir: [por integrar]")
    lines.append("")
    lines.append("Cambio (Exchange / Mercado Negro / Liquidez):")
    lines.append("\t• Cambio oficial del país → USD y EUR: [por integrar]")
    lines.append("\t• Cambio real de calle / mercado negro: [por integrar]")
    lines.append("\t• Disponibilidad de efectivo / colapsos bancarios / restricciones: [por integrar]")
    lines.append("\t• Impacto operativo: coste para convoyes, capacidad de compra de personal ONU/INGO, inflación y deterioro económico local.")
    lines.append("")

    # 5. SITUACIÓN MISIÓN ONU / AUTORIDADES / FUERZA MULTINACIONAL
    lines.append("⸻")
    lines.append("5. SITUACIÓN MISIÓN ONU / AUTORIDADES / FUERZA MULTINACIONAL")
    lines.append("\t• Postura de seguridad UNDSS / SIOC: [por integrar]")
    lines.append("\t• Riesgos para instalaciones y personal ONU: [por integrar]")
    lines.append("\t• Estado del despliegue multinacional (ISF, BINUH, MINUSMA, etc.): [por integrar]")
    lines.append("\t• Decisiones recientes del CSNU / Gobierno / Alianzas: [por integrar]")
    lines.append("\t• Actividad hostil contra personal ONU o INGO: [por integrar]")
    lines.append("\t• Cambios en reglas de movimiento / niveles de alerta: [por integrar]")
    lines.append("\t• Evaluación estratégica del día: [por integrar]")
    lines.append("")

    # 6. RECOMENDACIONES
    lines.append("⸻")
    lines.append("6. RECOMENDACIONES")
    lines.append("")
    lines.append("6.1 Seguridad y Movilidad")
    lines.append("\t• [Por completar manualmente]")
    lines.append("")
    lines.append("6.2 Humanitario / Hazards")
    lines.append("\t• [Por completar manualmente]")
    lines.append("")
    lines.append("6.3 Marco Político–Estratégico / ONU / Fuerza Multinacional")
    lines.append("\t• [Por completar manualmente]")
    lines.append("")

    return "\n".join(lines)


async def _run_sicu_full_for(
    bot,
    chat_id: int,
    raw_country: str,
    day: str,
) -> None:
    country_slug = _slugify_country(raw_country)
    await bot.send_message(
        chat_id=chat_id,
        text=f"⏳ Pipeline SICU para {raw_country.upper()} {day}…",
    )

    # ===== 1) TXT DÍA (ORIGINAL) =====
    txt_path = _country_dir(country_slug) / f"{day}.txt"
    if not txt_path.exists():
        await bot.send_message(
            chat_id=chat_id,
            text=f"❌ No hay TXT para {raw_country.upper()} en {day}.\nBuscado: {txt_path}",
        )
        return

    try:
        original_txt = txt_path.read_text(encoding="utf-8", errors="ignore")
    except Exception as e:
        await bot.send_message(
            chat_id=chat_id,
            text=f"❌ Error leyendo TXT {txt_path.name}: {e!r}",
        )
        return

    # Registrar incidentes desde TXT
    if original_txt.strip():
        ingest_country = country_slug.replace("_", " ").strip().title()
        try:
            registrados = registrar_incidentes_desde_texto(
                pais=ingest_country,
                texto_informe=original_txt,
                fuente=f"TXT {raw_country.upper()} {day}",
                resolver_ahora=True,
                country_hint=ingest_country,
            )
            print(f"[sicu_full] {country_slug} {day}: {registrados} incidentes registrados desde TXT")
        except Exception as e:
            print(f"[sicu_full] fallo registrando incidentes desde TXT: {e!r}")

    # Enviar TXT ORIGINAL
    try:
        with txt_path.open("rb") as fh:
            await bot.send_document(
                chat_id=chat_id,
                document=InputFile(fh, filename=f"{country_slug}-{day}.txt"),
                caption=f"{raw_country.upper()} :: {day} (TXT original)",
            )
    except Exception as e:
        await bot.send_message(
            chat_id=chat_id,
            text=f"⚠️ No se pudo enviar el TXT original: {e!r}",
        )

    # ===== 2) CSV INCIDENTES (TXT → CSV) =====
    try:
        csv_incidentes_path, total_inc = save_incidentes_csv_from_txt(country_slug, day)
        print(f"[sicu_full] CSV incidentes actualizado: {csv_incidentes_path} ({total_inc} filas)")
    except Exception as e:
        await bot.send_message(
            chat_id=chat_id,
            text=f"❌ Error generando CSV de incidentes: {e!r}",
        )
        return

    # Enviar CSV de incidentes
    try:
        with csv_incidentes_path.open("rb") as f:
            await bot.send_document(
                chat_id=chat_id,
                document=InputFile(f, filename=csv_incidentes_path.name),
                caption=f"📄 CSV INCIDENTES :: {raw_country.upper()} {day} ({total_inc} registros)",
            )
    except Exception as e:
        await bot.send_message(
            chat_id=chat_id,
            text=f"⚠️ CSV incidentes creado pero no enviado: {e!r}",
        )

    # ===== 3) CSV SICU + TXT SICU =====
    try:
        with csv_incidentes_path.open(newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            base_rows = list(reader)
    except Exception as e:
        await bot.send_message(
            chat_id=chat_id,
            text=f"❌ Error leyendo CSV incidentes {csv_incidentes_path.name}: {e!r}",
        )
        return

    if not base_rows:
        await bot.send_message(
            chat_id=chat_id,
            text="ℹ️ El CSV de incidentes está vacío. No hay eventos para clasificar.",
        )
        return

    normalizados: List[Dict[str, Any]] = []
    for r in base_rows:
        fecha = r.get("fecha") or r.get("Fecha") or day
        hora = r.get("hora") or r.get("Hora") or ""
        pais = r.get("pais") or r.get("Pais") or country_slug.capitalize()
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
        await bot.send_message(
            chat_id=chat_id,
            text=(
                f"ℹ️ No hay incidentes SICU categorizados para {raw_country.upper()} {day} "
                "(solo 'Otros' o sin descripción relevante)."
            ),
        )
        return

    # ✅ DEDUPLICACIÓN INTELIGENTE ANTES DE GENERAR CSV/TXT/INFORME
    filtrados = deduplicate_sicu_incidents(filtrados)

    # Ordenar por fecha/hora para salida ordenada
    filtrados.sort(key=lambda r: (r.get("fecha", ""), r.get("hora", "")))

    country_sicu_dir = CATEG_BASE_DIR / country_slug
    country_sicu_dir.mkdir(parents=True, exist_ok=True)

    csv_sicu_path = country_sicu_dir / f"{country_slug}-{day}_incidentes_SICU.csv"
    txt_sicu_path = country_sicu_dir / f"{country_slug}-{day}_incidentes_SICU.txt"

    # Guardar CSV SICU
    try:
        with csv_sicu_path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(
                f,
                fieldnames=["fecha", "hora", "pais", "categoria_sicu",
                            "descripcion", "localizacion", "lat", "lon", "fuente_URL"],
            )
            writer.writeheader()
            writer.writerows(filtrados)
    except Exception as e:
        await bot.send_message(
            chat_id=chat_id,
            text=f"❌ Error guardando CSV SICU: {e!r}",
        )
        return

    # Enviar CSV SICU
    try:
        with csv_sicu_path.open("rb") as f:
            await bot.send_document(
                chat_id=chat_id,
                document=InputFile(f, filename=csv_sicu_path.name),
                caption=f"📄 CSV SICU :: {raw_country.upper()} {day}",
            )
    except Exception as e:
        await bot.send_message(
            chat_id=chat_id,
            text=f"⚠️ CSV SICU creado pero no enviado: {e!r}",
        )

    # Guardar y enviar TXT SICU agrupado
    try:
        grouped = _group_by_category(filtrados)
        lines_txt: List[str] = []
        lines_txt.append("Sucesos / Incidentes (Clasificación SICU)\n")
        for cat in ("Conflicto Armado", "Terrorismo", "Criminalidad",
                    "Disturbios Civiles", "Hazards"):
            items = grouped.get(cat, [])
            if not items:
                continue
            loc_counts = Counter((it["localizacion"] or "Localización no especificada") for it in items)
            top_locs = ", ".join(f"{loc} ({n})" for loc, n in loc_counts.most_common(3))
            lines_txt.append(f"{cat}:")
            if top_locs:
                lines_txt.append(f"  Áreas principales: {top_locs}")
            for it in items:
                desc = it["descripcion"]
                loc = it["localizacion"] or "Localización no especificada"
                fuente = it.get("fuente_URL") or ""
                linea = f" - {desc} → {loc}"
                if fuente:
                    linea += f" | Fuente: {fuente}"
                lines_txt.append(linea)
            lines_txt.append("")
        txt_sicu_path.write_text("\n".join(lines_txt), encoding="utf-8")

        with txt_sicu_path.open("rb") as f:
            await bot.send_document(
                chat_id=chat_id,
                document=InputFile(f, filename=txt_sicu_path.name),
                caption=f"TXT SICU :: {raw_country.upper()} {day}",
            )
    except Exception as e:
        await bot.send_message(
            chat_id=chat_id,
            text=f"⚠️ Error generando/enviando TXT SICU: {e!r}",
        )

    # ===== Informe SICU (plantilla sin LLM) =====
    try:
        report_dir = OUTPUT_DIR / "sicu_reports" / country_slug
        report_dir.mkdir(parents=True, exist_ok=True)
        report_path = report_dir / f"{country_slug}-{day}_SICU_REPORT.txt"

        report_txt = _build_sicu_report_txt(raw_country, country_slug, day, filtrados)
        report_path.write_text(report_txt, encoding="utf-8")

        with report_path.open("rb") as f:
            await bot.send_document(
                chat_id=chat_id,
                document=InputFile(f, filename=report_path.name),
                caption=f"📄 INFORME SICU :: {raw_country.upper()} {day}",
            )
    except Exception as e:
        await bot.send_message(
            chat_id=chat_id,
            text=f"⚠️ Informe SICU generado pero no enviado: {e!r}",
        )

    # ===== KML desde CSV SICU =====
    try:
        kml_path_str = csv_to_kml(
            csv_path=str(csv_sicu_path),
            out_path=None,
            day_iso=day,
            enrich=False,  # sin enriquecimiento para ir más rápido
            country=country_slug,
        )
        kml_path = Path(kml_path_str)
        with kml_path.open("rb") as f:
            await bot.send_document(
                chat_id=chat_id,
                document=InputFile(f, filename=kml_path.name),
                caption=f"🗺️ KML SICU :: {raw_country.upper()} {day}",
            )
    except Exception as e:
        await bot.send_message(
            chat_id=chat_id,
            text=f"⚠️ Error generando/enviando KML SICU: {e!r}",
        )


async def sicu_full(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    /sicu_full <pais> <YYYY-MM-DD>  (uso manual)
    """
    args = context.args or []
    if len(args) < 2:
        return await update.message.reply_text(
            "Uso: /sicu_full <pais> <YYYY-MM-DD>\n"
            "Ejemplo: /sicu_full libia 2025-11-21"
        )

    raw_country = args[0].strip()
    day = args[1].strip()
    chat_id = update.effective_chat.id

    await _run_sicu_full_for(context.bot, chat_id, raw_country, day)


async def sicu_full_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Job programado. Se ejecuta sin Update, solo con context.
    - Usa la lista AUTO_SICU_COUNTRIES.
    - Usa la fecha del día actual.
    """
    chat_id = context.job.chat_id
    today = datetime.now().date()
    day = today.isoformat()

    bot = context.bot
    for country in AUTO_SICU_COUNTRIES:
        try:
            await _run_sicu_full_for(bot, chat_id, country, day)
        except Exception as e:
            print(f"[sicu_full_job] Error en país {country}: {e!r}")
            try:
                await bot.send_message(
                    chat_id=chat_id,
                    text=f"⚠️ Error en sicu_full_job para {country.upper()}: {e!r}",
                )
            except Exception:
                pass