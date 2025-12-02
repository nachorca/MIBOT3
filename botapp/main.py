from __future__ import annotations
# --- compat: permitir ejecutar este archivo como script ---
if __package__ is None or __package__ == "":
    import sys, pathlib
    sys.path.append(str(pathlib.Path(__file__).resolve().parents[1]))

import logging
import asyncio
import atexit
from datetime import time as dtime

from botapp.handlers.generate_report import generate_report_step1

from botapp.handlers.generate_report import generate_report_step2

from botapp.handlers.map import map_incidentes
from botapp.handlers.sicu_map import sicu_map_cmd
from botapp.handlers.audit_geo import audit_csv_cmd
from botapp.handlers.incidentes_categorizados import incidentes_categorizados
from botapp.handlers.scrape import scrape, scrape_all, scrape_auto_job
from botapp.handlers.incidentes import incidentes_resolve
from botapp.handlers.incidentes_csv import incidentes_csv
from botapp.handlers.csv_to_kml import csv_to_kml_cmd
from botapp.handlers.scrape_x import scrape_x, scrape_x_job
from botapp.handlers.sicu_map import get_handlers as get_sicu_map_handlers
from botapp.handlers.sicu_full import sicu_full, sicu_full_job  # 👈 ya incluye sicu_full y sicu_full_job
from botapp.handlers.sicu_ai import sicu_ai


import pytz
from telegram import Update
from telegram.ext import Application, CommandHandler
from telegram.error import Conflict, NetworkError
import sys
import fcntl
from typing import Optional, TextIO

from botapp.config import get_settings
from botapp.utils.logging import setup_logging

# Handlers básicos
from botapp.handlers.basic import start, help_cmd, ping, status

# TXT / METEO / NOTAM / NEWS
from botapp.handlers.report import txt_cmd, add_test_entry
from botapp.handlers.meteo import meteo
from botapp.handlers.notam import notam  # si no lo tienes, elimina esta línea
from botapp.handlers.news import news
from botapp.handlers.channels import addchannel, delchannel, listchannels, checkchannels

# Canales / Recolector
from botapp.handlers.collect import (
    collect,           # NUEVO: wrapper /collect
    collect_on,
    collect_off,
    collect_now,
    reset_daily_job,
    collect_fetch_week,
)

# Días / Rangos
from botapp.handlers.days import txtdia, txtrango, txtsemana, zipsemana, txtdia_es

# Periodos operativos (07:00–06:59)
from botapp.handlers.periods import report_dia, report_semana, report_quincena, report_mes, zip_period

# Vuelos
from botapp.handlers.flights import flights, route_add, route_list, flights_addroutes_bootstrap

# ===== Menú de comandos (Telegram command menu) =====
COMMANDS_MENU = [
    ("start", "Inicia el bot"),
    ("help", "Muestra ayuda"),
    ("ping", "Comprueba si el bot responde"),
    ("status", "Estado del bot"),
    ("txt", "Obtener TXT del día"),
    ("add_test_entry", "Añadir entrada de prueba"),
    ("meteo", "Consultar meteorología"),
    ("notam", "Añadir NOTAM (ICAO) al TXT"),
    ("news", "Añadir titulares desde una URL"),
    ("addchannel", "Añadir canal a un país"),
    ("delchannel", "Eliminar canal de un país"),
    ("listchannels", "Listar canales de un país"),
    ("checkchannels", "Diagnosticar canales registrados"),
    ("collect", "Gestor recolector: /collect on|off|now|week|status"),
    ("collect_on", "Activar recolector"),
    ("collect_off", "Desactivar recolector"),
    ("collect_now", "Recolector inmediato"),
    ("collect_fetch_week", "Importar histórico por bloques"),
    ("txtdia", "TXT de un día específico"),
    ("txtrango", "TXT de un rango de días"),
    ("txtsemana", "TXT de últimos n días"),
    ("zipsemana", "ZIP de últimos n días"),
    ("reportdia", "Reporte del día operativo"),
    ("reportsemana", "Reporte de la semana"),
    ("reportquincena", "Reporte de 15 días"),
    ("reportmes", "Reporte del mes operativo"),
    ("zipperiod", "ZIP de un periodo"),
    ("flights", "Buscar vuelos (ida/ida+vuelta)"),
    ("route_add", "Añadir ruta"),
    ("route_list", "Listar rutas"),
    ("scrape", "Raspar fuentes configuradas"),
    ("scrape_all", "Raspar todas las fuentes por país"),
    ("scrape_x", "Tweets de X (snscrape)"),
    ("incidentes_resolve", "Geocodificar incidentes pendientes"),
    ("incidentes_csv", "Exportar incidentes a CSV"),
    ("map_incidentes", "Mapa de incidentes (SICU)"),
    ("sicu_map", "Generar mapa SICU desde CSV (Leaflet)"),
    ("incidentes_categorizados", "Clasifica incidentes (SICU) desde output"),
    ("csv_to_kml", "CSV → KML (SICU)"),
    ("audit_csv", "Auditar CSV (geolocalización)"),
    ("sicu_full", "Pipeline completo TXT+CSV+SICU+KML"),
]

async def post_init(application: Application) -> None:
    # Establecer menú de comandos de forma asíncrona (evita 'never awaited' en Py 3.13)
    try:
        await application.bot.set_my_commands(COMMANDS_MENU)
    except Exception as e:
        print(f"⚠️ set_my_commands falló (se continúa): {e!r}")

async def on_error(update, context):
    print(f"❗ Error: {context.error!r}")

_LOCK_FP: Optional[TextIO] = None

def _release_lock() -> None:
    global _LOCK_FP
    if _LOCK_FP is None:
        return
    try:
        fcntl.lockf(_LOCK_FP, fcntl.LOCK_UN)
    except OSError:
        pass
    try:
        _LOCK_FP.close()
    except Exception:
        pass
    _LOCK_FP = None

atexit.register(_release_lock)

def single_instance(lockfile: str = ".mibot.lock") -> None:
    """
    Evita dobles arranques del bot. Si ya hay otra instancia, sale con aviso.
    """
    global _LOCK_FP
    if _LOCK_FP is not None:
        return
    try:
        fp = open(lockfile, "w")
    except Exception as e:
        print(f"⚠️ No se pudo abrir lockfile {lockfile}: {e!r}")
        return
    try:
        fcntl.lockf(fp, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        print("⚠️ Ya hay otra instancia del bot en ejecución. Saliendo.")
        sys.exit(1)
    _LOCK_FP = fp

def build_app() -> Application:
    settings = get_settings()
    setup_logging(logging.DEBUG)

    app = (
        Application
        .builder()
        .token(settings.telegram_bot_token)
        .post_init(post_init)
        .build()
    )

    # Básicos
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_cmd))
    app.add_handler(CommandHandler("ping", ping))
    app.add_handler(CommandHandler("status", status))

    # TXT / METEO / NOTAM / NEWS
    app.add_handler(CommandHandler("txt", txt_cmd))
    app.add_handler(CommandHandler("add_test_entry", add_test_entry))
    app.add_handler(CommandHandler("meteo", meteo))
    try:
        app.add_handler(CommandHandler("notam", notam))
    except Exception:
        pass  # si no implementaste notam todavía
    app.add_handler(CommandHandler("news", news))

    # Canales / Recolector
    app.add_handler(CommandHandler("addchannel", addchannel))
    app.add_handler(CommandHandler("delchannel", delchannel))
    app.add_handler(CommandHandler("listchannels", listchannels))
    app.add_handler(CommandHandler("checkchannels", checkchannels))

    app.add_handler(CommandHandler("collect", collect))
    app.add_handler(CommandHandler("collect_on", collect_on))
    app.add_handler(CommandHandler("collect_off", collect_off))
    app.add_handler(CommandHandler("collect_now", collect_now))
    app.add_handler(CommandHandler("collect_fetch_week", collect_fetch_week))

    # Días / Rangos
    app.add_handler(CommandHandler("txtdia", txtdia))
    app.add_handler(CommandHandler("txtrango", txtrango))
    app.add_handler(CommandHandler("txtsemana", txtsemana))
    app.add_handler(CommandHandler("zipsemana", zipsemana))
    app.add_handler(CommandHandler("txtdia_es", txtdia_es))

    # Periodos (07:00–06:59)
    app.add_handler(CommandHandler("reportdia", report_dia))
    app.add_handler(CommandHandler("reportsemana", report_semana))
    app.add_handler(CommandHandler("reportquincena", report_quincena))
    app.add_handler(CommandHandler("reportmes", report_mes))
    app.add_handler(CommandHandler("zipperiod", zip_period))

    # Vuelos
    app.add_handler(CommandHandler("flights", flights))
    app.add_handler(CommandHandler("route_add", route_add))
    app.add_handler(CommandHandler("route_list", route_list))

    # Scraping / mapas / CSV
    app.add_handler(CommandHandler("scrape", scrape))
    app.add_handler(CommandHandler("scrape_all", scrape_all))
    app.add_handler(CommandHandler("scrape_x", scrape_x))
    app.add_handler(CommandHandler("scrape_x_job", scrape_x_job))

    app.add_handler(CommandHandler("incidentes_resolve", incidentes_resolve))
    app.add_handler(CommandHandler("incidentes_csv", incidentes_csv))
    app.add_handler(CommandHandler("incidentes_categorizados", incidentes_categorizados))
    app.add_handler(CommandHandler("map_incidentes", map_incidentes))
    app.add_handler(CommandHandler("sicu_map", sicu_map_cmd))
    app.add_handler(CommandHandler("csv_to_kml", csv_to_kml_cmd))
    app.add_handler(CommandHandler("audit_csv", audit_csv_cmd))
    app.add_handler(CommandHandler("sicu_full", sicu_full))
    app.add_handler(CommandHandler("sicu_full_job", sicu_full_job))
    app.add_handler(CommandHandler("generate_report", generate_report_step1))
    app.add_handler(CommandHandler("generate_report_step2", generate_report_step2))
    app.add_handler(CommandHandler("sicu_ai", sicu_ai))

    # SICU map (CSV -> HTML Leaflet)
    for h in get_sicu_map_handlers():
        app.add_handler(h)

    # Job diario 07:00 (reset + cabeceras)
    tz = pytz.timezone(settings.tz or "Africa/Tripoli")
    app.job_queue.run_daily(
        reset_daily_job,
        time=dtime(hour=23, minute=59, second=59, tzinfo=tz),
        name="reset:23:59:59",
        chat_id=None,
    )
    if app.job_queue is not None:
        # Chat donde quieres recibir el resumen del scraping automático
        # Puedes cambiar este ID si quieres otro chat
        SCRAPE_CHAT_ID = 1143799793

        app.job_queue.run_repeating(
            scrape_auto_job,
            interval=900,  # 15 minutos
            first=60,
            name="scrape:auto",
            chat_id=SCRAPE_CHAT_ID,
            job_kwargs={
                "max_instances": 2,
                "coalesce": True,
                "misfire_grace_time": 600,  # margen ante retrasos
            },
        )

        # === Jobs automáticos SICU_FULL (10:00, 12:00, 14:00, 18:00, 21:00) ===
        # ⚠️ Sustituye este chat_id por el de tu chat con el bot
        SICU_CHAT_ID = 1143799793  # TODO: pon aquí tu chat_id numérico

        for hour in (10, 12, 14, 18, 21):
            app.job_queue.run_daily(
                sicu_full_job,
                time=dtime(hour=hour, minute=0, second=0, tzinfo=tz),
                name=f"sicu_full_auto_{hour}",
                chat_id=SICU_CHAT_ID,
            )

    app.add_error_handler(on_error)
    return app

def main():
    single_instance()
    # Bootstrap de rutas de vuelos por defecto
    try:
        import inspect, asyncio as _asyncio
        if inspect.iscoroutinefunction(flights_addroutes_bootstrap):
            _asyncio.run(flights_addroutes_bootstrap())
        else:
            flights_addroutes_bootstrap()
    except Exception as e:
        print(f"⚠️ Bootstrap rutas falló (se continúa): {e!r}")

    app = build_app()
    print("🤖 Bot arrancado. Esperando mensajes…")
    # Crear un único event loop para eliminar webhook y ejecutar polling.
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        try:
            loop.run_until_complete(app.bot.delete_webhook(drop_pending_updates=True))
            print("ℹ️ Webhook eliminado (si existía).")
        except Exception as e:
            # No fatal: mostrar advertencia y continuar con polling (delete_webhook puede fallar por red)
            print(f"⚠️ No se pudo eliminar webhook antes de polling: {e!r}")

        try:
            loop.run_until_complete(app.run_polling(allowed_updates=Update.ALL_TYPES))
        except Conflict as c:
            print("❗ Error: Conflict con getUpdates: parece que otro proceso o un webhook está activo.")
            print("Asegúrate de que no hay otro bot en ejecución y que el webhook (si existía) fue eliminado.")
            print(f"Detalle: {c!r}")
            sys.exit(1)
        except NetworkError as net_err:
            print("❗ No se pudo conectar con la API de Telegram.")
            print(f"   Detalle: {net_err}")
            print("   Verifica conexión a Internet/VPN y que el token sea correcto.")
            sys.exit(1)
    finally:
        try:
            loop.close()
        except Exception:
            pass


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n🛑 Bot detenido manualmente por el usuario.")