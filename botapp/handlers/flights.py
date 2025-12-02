from __future__ import annotations
import json
from pathlib import Path
from telegram import Update
from telegram.ext import ContextTypes

from ..config import get_settings
from ..services.flights import FlightsService, SearchParams

SET = get_settings()
FLIGHTS = FlightsService()
ROUTES_FILE = Path(SET.data_dir) / "routes.json"

def _load_routes() -> dict:
    if not ROUTES_FILE.exists():
        return {}
    try:
        return json.loads(ROUTES_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {}

def _save_routes(d: dict) -> None:
    ROUTES_FILE.write_text(json.dumps(d, ensure_ascii=False, indent=2), encoding="utf-8")

async def flights_addroutes_bootstrap() -> None:
    """
    Crea dos rutas base si no existen:
      - tunez-madrid
      - tripoli-madrid
    """
    routes = _load_routes()
    changed = False
    if "tunez-madrid" not in routes:
        routes["tunez-madrid"] = {"origin": "tunez", "destination": "madrid"}
        changed = True
    if "tripoli-madrid" not in routes:
        routes["tripoli-madrid"] = {"origin": "tripoli", "destination": "madrid"}
        changed = True
    if changed:
        _save_routes(routes)

async def route_add(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    /route_add <alias> <origen> <destino>
    Ej: /route_add tunez-madrid tunez madrid
    """
    if len(context.args) < 3:
        return await update.message.reply_text("Uso: /route_add <alias> <origen> <destino>")
    alias = context.args[0].lower().strip()
    origin = context.args[1].lower().strip()
    dest = context.args[2].lower().strip()

    routes = _load_routes()
    routes[alias] = {"origin": origin, "destination": dest}
    _save_routes(routes)
    await update.message.reply_text(f"✅ Ruta '{alias}' guardada: {origin} → {dest}")

async def route_list(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    /route_list
    """
    routes = _load_routes()
    if not routes:
        return await update.message.reply_text("No hay rutas. Añade con /route_add")
    lines = [f"• {k}: {v['origin']} → {v['destination']}" for k, v in routes.items()]
    await update.message.reply_text("\n".join(lines))

async def flights(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    /flights <ruta|origen-destino> <YYYY-MM-DD> [YYYY-MM-DD] [economico|rapido]
    Ej:
      /flights tunez-madrid 2025-09-20 2025-09-25 economico
      /flights tripoli-madrid 2025-09-21 rapido            (solo ida, rápido)
      /flights tunez-madrid 2025-09-21                      (solo ida, económico)
      /flights tunez-madrid 2025-09-21 2025-09-25          (ida y vuelta, económico)
    """
    if len(context.args) < 2:
        return await update.message.reply_text(
            "Uso: /flights <ruta|origen-destino> <YYYY-MM-DD> [YYYY-MM-DD] [economico|rapido]"
        )

    # Resolver alias o "origen-destino"
    route_key = context.args[0].lower().strip()
    routes = _load_routes()
    if route_key in routes:
        origin = routes[route_key]["origin"]
        dest = routes[route_key]["destination"]
    else:
        if "-" not in route_key:
            return await update.message.reply_text("Formato de ruta inválido. Usa un alias o <origen-destino> (p.ej., tunez-madrid).")
        origin, dest = route_key.split("-", 1)

    depart_date = context.args[1].strip()
    return_date = None
    preference = "economico"
    provider = None

    if len(context.args) >= 3:
        # ¿tercer argumento es fecha de vuelta o preferencia?
        a2 = context.args[2].lower().strip()
        if a2 in ("economico", "rápido", "rapido"):
            preference = "rapido" if a2.startswith("r") else "economico"
        elif len(context.args[2]) == 10 and context.args[2].count("-") == 2:
            return_date = context.args[2]
            if len(context.args) >= 4:
                a3 = context.args[3].lower().strip()
                if a3 in ("economico", "rápido", "rapido"):
                    preference = "rapido" if a3.startswith("r") else "economico"
    # Selector de proveedor opcional (último arg): provider=amadeus|tequila|dummy o token suelto
    if context.args:
        last = context.args[-1].lower().strip()
        if last.startswith("provider="):
            pv = last.split("=", 1)[1]
            if pv in ("amadeus", "tequila", "dummy"):
                provider = pv
        elif last in ("amadeus", "tequila", "dummy"):
            provider = last

    params = SearchParams(
        origin=origin,
        destination=dest,
        depart_date=depart_date,
        return_date=return_date,
        preference=preference,
        provider=provider,
    )
    results = await FLIGHTS.search(params)
    if not results:
        return await update.message.reply_text("No se encontraron itinerarios.")

    # Render corto (top 5)
    lines = []
    for it in results[:5]:
        if it.in_flight:
            txt = (
                f"💺 {it.out_flight.carrier}{it.out_flight.flight_number} {it.out_flight.depart_airport}→{it.out_flight.arrive_airport} "
                f"{it.out_flight.depart_dt:%Y-%m-%d %H:%M} → {it.out_flight.arrive_dt:%H:%M} (stops:{it.out_flight.stops})\n"
                f"↩️ {it.in_flight.carrier}{it.in_flight.flight_number} {it.in_flight.depart_airport}→{it.in_flight.arrive_airport} "
                f"{it.in_flight.depart_dt:%Y-%m-%d %H:%M} → {it.in_flight.arrive_dt:%H:%M} (stops:{it.in_flight.stops})\n"
                f"💶 {it.total_price:.2f} {it.currency} • 🕒 llega {it.final_arrival:%Y-%m-%d %H:%M}\n"
            )
            if it.booking_url:
                txt += f"🔗 Reserva: {it.booking_url}\n"
            txt += "—"
            lines.append(txt)
        else:
            txt = (
                f"💺 {it.out_flight.carrier}{it.out_flight.flight_number} {it.out_flight.depart_airport}→{it.out_flight.arrive_airport} "
                f"{it.out_flight.depart_dt:%Y-%m-%d %H:%M} → {it.out_flight.arrive_dt:%H:%M} (stops:{it.out_flight.stops})\n"
                f"💶 {it.total_price:.2f} {it.currency} • 🕒 llega {it.final_arrival:%Y-%m-%d %H:%M}\n"
            )
            if it.booking_url:
                txt += f"🔗 Reserva: {it.booking_url}\n"
            txt += "—"
            lines.append(txt)
    await update.message.reply_text("\n".join(lines))