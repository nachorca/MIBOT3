# -*- coding: utf-8 -*-
from __future__ import annotations
import os, json
from datetime import datetime, timezone
from typing import Optional, Dict, Any, List, Tuple

try:
    import aiohttp
except Exception:  # pragma: no cover - facilita import sin deps en análisis estático
    aiohttp = None

# =========================
#  Configuración / Constantes
# =========================

# AEMET (Campello)
AEMET_MUNI_CAMPELLO = "03050"  # El Campello (AEMET)

# Coordenadas por país (fallback OWM)
COUNTRY_COORDS: Dict[str, Tuple[float, float]] = {
    "libia": (32.8872, 13.1913),          # Trípoli
    "haiti": (18.5944, -72.3074),         # Port-au-Prince
    "colombia": (5.6947, -76.6611),       # Quibdó (ejemplo)
    "campello": (38.4288, -0.3977),       # El Campello
    "gaza": (31.5219, 34.4440),           # Gaza City
}

# Emojis compatibles
EMO = {
    "TIME": "⏰",
    "TEMP": "🌡️",
    "WIND": "💨",
    "PRESS": "🔽",
    "VIS": "👁️",
    "CLOUD": "☁️",
    "UV": "🔆",
    "RAIN": "☔",
    "SNOW": "❄️",
    "MINI": "🔎",
    "FEELS": "≈",
}

# =========================
#  Helpers
# =========================

def _fmt(val: Optional[float], suf: str = "", nd: int = 0) -> str:
    if val is None:
        return "—"
    if isinstance(val, (int, float)):
        return f"{val:.{nd}f}{suf}" if nd else f"{int(round(val))}{suf}"
    return "—"

def _wind_dir(deg: Optional[float]) -> str:
    if deg is None:
        return "—"
    dirs = ["N","NNE","NE","ENE","E","ESE","SE","SSE",
            "S","SSW","SW","WSW","W","WNW","NW","NNW"]
    i = int((deg/22.5)+0.5) % 16
    return dirs[i]

def _now_iso() -> str:
    return datetime.now(timezone.utc).astimezone().strftime("%Y-%m-%d %H:%M")

def _uvi_to_text(u: Optional[float]) -> str:
    if u is None: return "—"
    uvi = float(u)
    if uvi < 3: lvl = "bajo"
    elif uvi < 6: lvl = "moderado"
    elif uvi < 8: lvl = "alto"
    elif uvi < 11: lvl = "muy alto"
    else: lvl = "extremo"
    return f"{uvi:.1f} ({lvl})"

# =========================
#  AEMET (Campello)
# =========================

async def fetch_aemet_campello(session: Any, api_key: str) -> Dict[str, Any]:
    """
    AEMET opendata: 2 pasos (meta -> datos). El segundo suele venir como text/plain ISO-8859-15.
    """
    base = "https://opendata.aemet.es/opendata/api"
    url = f"{base}/prediccion/especifica/municipio/diaria/{AEMET_MUNI_CAMPELLO}/?api_key={api_key}"

    async with session.get(url, headers={"Accept": "application/json"}, timeout=30) as r:
        meta = await r.json(content_type=None)
    if not isinstance(meta, dict) or not meta.get("datos"):
        raise RuntimeError(f"AEMET meta inválido: {meta}")

    datos_url = meta["datos"]
    async with session.get(datos_url, timeout=30) as r2:
        txt = await r2.text(encoding="iso-8859-15", errors="ignore")
        try:
            data = json.loads(txt)
        except json.JSONDecodeError:
            try:
                data = await r2.json(content_type=None)
            except Exception:
                raise RuntimeError(f"AEMET datos no-JSON: {r2.content_type}")

    return {"raw": data}

def build_block_meteo_aemet(aemet: Dict[str, Any]) -> str:
    raw = aemet.get("raw") or []
    if not raw:
        return "=== METEO ESPAÑA (AEMET) ===\nNo datos.\n=== FIN METEO ===\n\n"

    dia = raw[0].get("prediccion", {}).get("dia", [])
    if not dia:
        return "=== METEO ESPAÑA (AEMET) ===\nNo datos.\n=== FIN METEO ===\n\n"

    d0 = dia[0]
    tmax = d0.get("temperatura", {}).get("maxima")
    tmin = d0.get("temperatura", {}).get("minima")
    nub = d0.get("estadoCielo", [])
    nub_desc = ", ".join(sorted({e.get("descripcion","") for e in nub if e.get("descripcion")})) or "—"
    pp = d0.get("probPrecipitacion", [])
    pp_vals = [int(x["value"]) for x in pp if str(x.get("value","")).isdigit()]
    pp_max = f"{max(pp_vals)}%" if pp_vals else "—"

    tramos = []
    for e in nub:
        per = e.get("periodo")
        desc = e.get("descripcion")
        if per and desc:
            tramos.append(f"{per}: {desc}")
    mini = "\n".join(tramos[:4]) if tramos else "Sin tramos."

    body = [
        "=== METEO ESPAÑA (AEMET) :: CAMPELLO ===",
        f"{EMO['TIME']} {_now_iso()}",
        f"{EMO['TEMP']} Temp máx/mín: {_fmt(tmax,'°C')} / {_fmt(tmin,'°C')}",
        f"{EMO['CLOUD']} Nubosidad: {nub_desc}",
        f"{EMO['RAIN']} PP (máx tramos): {pp_max}",
        f"{EMO['MINI']} Mini-pronóstico 12h:\n{mini}",
        "=== FIN METEO ===",
        ""
    ]
    return "\n".join(body)

# =========================
#  OWM (OneCall + Fallback)
# =========================

async def fetch_owm(session: Any, api_key: str, lat: float, lon: float) -> Dict[str, Any]:
    """
    OpenWeather OneCall (2.5): current + hourly + daily.
    """
    params = {
        "lat": lat, "lon": lon, "appid": api_key,
        "units": "metric", "lang": "es",
        "exclude": "minutely,alerts"
    }
    url = "https://api.openweathermap.org/data/2.5/onecall"
    async with session.get(url, params=params, timeout=30) as r:
        r.raise_for_status()  # puede lanzar ClientResponseError (p.ej. 401)
        data = await r.json(content_type=None)
    return {"raw": data}

async def fetch_owm_fallback(session: Any, api_key: str, lat: float, lon: float) -> Dict[str, Any]:
    """
    Fallback a weather + forecast si OneCall no está disponible (p.ej. 401).
    Devuelve estructura compatible con build_block_meteo_owm().
    """
    base = "https://api.openweathermap.org/data/2.5"
    params = {"appid": api_key, "units": "metric", "lang": "es"}

    # Weather actual
    async with session.get(f"{base}/weather", params={**params, "lat": lat, "lon": lon}, timeout=30) as r:
        r.raise_for_status()
        cur = await r.json(content_type=None)

    # Forecast 5 días (3h intervalos) - cogemos los 4 primeros tramos (~12h)
    async with session.get(f"{base}/forecast", params={**params, "lat": lat, "lon": lon, "cnt": 4}, timeout=30) as r2:
        r2.raise_for_status()
        forecast = await r2.json(content_type=None)

    raw = {
        "current": {
            "temp": cur.get("main", {}).get("temp"),
            "feels_like": cur.get("main", {}).get("feels_like"),
            "pressure": cur.get("main", {}).get("pressure"),
            "humidity": cur.get("main", {}).get("humidity"),
            "wind_speed": cur.get("wind", {}).get("speed"),
            "wind_deg": cur.get("wind", {}).get("deg"),
            "clouds": cur.get("clouds", {}).get("all"),
            "uvi": None,  # /weather no lo trae
            "visibility": cur.get("visibility"),
            "weather": cur.get("weather", []),
        },
        "hourly": []
    }

    for f in forecast.get("list", []):
        raw["hourly"].append({
            "dt": f.get("dt"),
            "temp": f.get("main", {}).get("temp"),
            "feels_like": f.get("main", {}).get("feels_like"),
            "pressure": f.get("main", {}).get("pressure"),
            "humidity": f.get("main", {}).get("humidity"),
            "wind_speed": f.get("wind", {}).get("speed"),
            "wind_deg": f.get("wind", {}).get("deg"),
            "clouds": f.get("clouds", {}).get("all"),
            "pop": f.get("pop", 0),
            "weather": f.get("weather", []),
        })
    return {"raw": raw}

def build_block_meteo_owm(country: str, owm: Dict[str, Any]) -> str:
    raw = owm.get("raw", {})
    cur = raw.get("current", {}) or {}
    hourly = raw.get("hourly", []) or []

    temp = cur.get("temp")
    feels = cur.get("feels_like")
    wind = cur.get("wind_speed")
    wind_deg = cur.get("wind_deg")
    press = cur.get("pressure")
    vis = cur.get("visibility")
    clouds = cur.get("clouds")
    uvi = cur.get("uvi")
    pop0 = hourly[0].get("pop") if hourly else None
    snow = None
    if "snow" in cur:
        snow = cur["snow"].get("1h", 0)

    # Mini-pronóstico 6–12h (4 tramos)
    lines = []
    for h in hourly[:4]:
        dt = datetime.fromtimestamp(h["dt"], tz=timezone.utc).astimezone()
        desc = (h.get("weather", [{}])[0].get("description") or "—").capitalize()
        t = _fmt(h.get("temp"), "°C")
        p = f"{int(round(h.get('pop', 0)*100))}%"
        lines.append(f"{dt.strftime('%H:%M')}: {desc}, {t}, PP {p}")
    mini = "\n".join(lines) if lines else "Sin tramos."

    body = [
        f"=== METEO {country.upper()} (OWM) ===",
        f"{EMO['TIME']} {_now_iso()}",
        f"{EMO['TEMP']} Temp: {_fmt(temp,'°C',1)}   {EMO['FEELS']} ST: {_fmt(feels,'°C',1)}",
        f"{EMO['WIND']} Viento: {_fmt(wind,' m/s',1)} {_wind_dir(wind_deg)}",
        f"{EMO['PRESS']} Presión: {_fmt(press,' hPa')}",
        f"{EMO['VIS']} Visibilidad: {_fmt((vis/1000.0 if vis else None),' km',1)}",
        f"{EMO['CLOUD']} Nubosidad: {_fmt(clouds,'%',0)}   {EMO['UV']} UV: {_uvi_to_text(uvi)}",
        f"{EMO['RAIN']} PP (próx hr): {f'{int(round(pop0*100))}%' if isinstance(pop0,(int,float)) else '—'}   {EMO['SNOW']} Nieve: {_fmt(snow,' mm',1)}",
        f"{EMO['MINI']} Mini-pronóstico 6–12h:\n{mini}",
        "=== FIN METEO ===",
        ""
    ]
    return "\n".join(body)

# =========================
#  Orquestador
# =========================

async def get_weather_block(country: str) -> str:
    """
    Decide proveedor por país y devuelve bloque METEO.
    - campello -> AEMET
    - otros -> OWM OneCall; si 401, fallback a weather+forecast
    """
    country = country.lower().strip()
    aemet_key = os.getenv("AEMET_API_KEY", "").strip()
    owm_key = os.getenv("OWM_API_KEY", "").strip()

    if aiohttp is None:
        return f"=== METEO {country.upper()} ===\nDependencia 'aiohttp' no instalada. Instala requirements.txt.\n=== FIN METEO ===\n\n"

    async with aiohttp.ClientSession() as session:
        if country == "campello":
            if not aemet_key:
                return "=== METEO ESPAÑA (AEMET) ===\nFalta AEMET_API_KEY en .env\n=== FIN METEO ===\n\n"
            try:
                data = await fetch_aemet_campello(session, aemet_key)
                return build_block_meteo_aemet(data)
            except Exception as e:
                return f"=== METEO ESPAÑA (AEMET) ===\nError: {e}\n=== FIN METEO ===\n\n"
        else:
            if not owm_key:
                return f"=== METEO {country.upper()} (OWM) ===\nFalta OWM_API_KEY en .env\n=== FIN METEO ===\n\n"

            lat, lon = COUNTRY_COORDS.get(country, COUNTRY_COORDS["libia"])
            try:
                data = await fetch_owm(session, owm_key, lat, lon)
                return build_block_meteo_owm(country, data)
            except Exception as e:
                # Si el error tiene atributo 'status' (p.ej. ClientResponseError), comprobar 401/403
                status = getattr(e, "status", None)
                if status in (401, 403):
                    try:
                        data = await fetch_owm_fallback(session, owm_key, lat, lon)
                        return build_block_meteo_owm(country, data)
                    except Exception as e2:
                        return f"=== METEO {country.upper()} (OWM fallback) ===\nError: {e2}\n=== FIN METEO ===\n\n"
                return f"=== METEO {country.upper()} (OWM) ===\nError: {e}\n=== FIN METEO ===\n\n"
            except Exception as e:
                # Cualquier otro error (red, parseo, etc.)
                try:
                    data = await fetch_owm_fallback(session, owm_key, lat, lon)
                    return build_block_meteo_owm(country, data)
                except Exception:
                    return f"=== METEO {country.upper()} (OWM) ===\nError: {e}\n=== FIN METEO ===\n\n"
