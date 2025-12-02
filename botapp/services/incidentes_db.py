# -*- coding: utf-8 -*-
from __future__ import annotations
import sqlite3
import time
from pathlib import Path
from typing import Optional, Tuple, List, Dict, Any
from datetime import datetime

try:
    from ..config import get_settings
except ImportError:
    from botapp.config import get_settings


def _db_path() -> Path:
    SET = get_settings()
    p = Path(SET.data_dir) / "incidentes.sqlite3"
    p.parent.mkdir(parents=True, exist_ok=True)
    return p


def _connect() -> sqlite3.Connection:
    """Abre una conexión SQLite configurada para minimizar bloqueos.

    Estrategias:
    - journal_mode=WAL permite concurrencia lectura/escritura.
    - synchronous=NORMAL reduce fsync extra sin perder durabilidad razonable.
    - busy_timeout + timeout de conexión amplían ventana de espera antes de lanzar 'database is locked'.
    """
    conn = sqlite3.connect(str(_db_path()), timeout=30)  # timeout de alto nivel
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    try:
        cur.execute("PRAGMA journal_mode=WAL;")
        cur.execute("PRAGMA synchronous=NORMAL;")
        cur.execute("PRAGMA busy_timeout=5000;")  # ms
    except Exception:
        pass  # pragma best-effort
    return conn


def _retry_locked(fn, *args, **kwargs):
    """Ejecuta fn con reintentos exponenciales si la BD está bloqueada."""
    max_tries = kwargs.pop("_max_tries", 6)
    base_sleep = kwargs.pop("_base_sleep", 0.05)
    for attempt in range(max_tries):
        try:
            return fn(*args, **kwargs)
        except sqlite3.OperationalError as e:
            msg = str(e).lower()
            if "locked" in msg or "busy" in msg:
                if attempt == max_tries - 1:
                    raise
                time.sleep(base_sleep * (2 ** attempt))
                continue
            raise


def init_db() -> None:
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS incidentes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                pais TEXT,
                categoria TEXT,
                descripcion TEXT,
                fuente TEXT,
                lat REAL,
                lon REAL,
                place TEXT,
                admin1 TEXT,
                admin2 TEXT,
                accuracy TEXT,
                geocode_source TEXT,
                created_at TEXT,
                updated_at TEXT
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS geocache (
                key TEXT PRIMARY KEY,
                lat REAL,
                lon REAL,
                country TEXT,
                admin1 TEXT,
                admin2 TEXT,
                accuracy TEXT,
                source TEXT,
                updated_at TEXT
            )
            """
        )
        conn.commit()
    finally:
        conn.close()


def migrate_db() -> None:
    # Placeholder para futuras migraciones.
    return


def incidente_exists(
    *,
    pais: str,
    categoria: str,
    descripcion: str,
    place: Optional[str] = None,
) -> bool:
    """
    Determina si ya existe un incidente con misma combinación básica.
    Coincidencia por país (case-insensitive), categoría, descripción (trim) y place (si se aporta).
    """
    desc = descripcion.strip()
    place_clean = (place or "").strip()
    conn = _connect()
    try:
        cur = conn.cursor()
        if place_clean:
            cur.execute(
                """
                SELECT 1
                FROM incidentes
                WHERE LOWER(pais) = LOWER(?)
                  AND categoria = ?
                  AND TRIM(descripcion) = ?
                  AND TRIM(COALESCE(place, '')) = ?
                LIMIT 1
                """,
                (pais, categoria, desc, place_clean),
            )
        else:
            cur.execute(
                """
                SELECT 1
                FROM incidentes
                WHERE LOWER(pais) = LOWER(?)
                  AND categoria = ?
                  AND TRIM(descripcion) = ?
                  AND (place IS NULL OR TRIM(place) = '')
                LIMIT 1
                """,
                (pais, categoria, desc),
            )
        return cur.fetchone() is not None
    finally:
        conn.close()


def add_incidente(
    *,
    pais: str,
    categoria: str,
    descripcion: str,
    fuente: str,
    lat: Optional[float] = None,
    lon: Optional[float] = None,
    place: Optional[str] = None,
) -> int:
    now = datetime.utcnow().isoformat(timespec="seconds")
    def _op():
        conn = _connect()
        try:
            cur = conn.cursor()
            cur.execute(
                """
                INSERT INTO incidentes (pais, categoria, descripcion, fuente, lat, lon, place, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (pais, categoria, descripcion, fuente, lat, lon, place, now, now),
            )
            conn.commit()
            return int(cur.lastrowid)
        finally:
            conn.close()

    return _retry_locked(_op)


def get_incidentes_pendientes() -> List[Dict[str, Any]]:
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT * FROM incidentes
            WHERE (lat IS NULL OR lon IS NULL)
              AND place IS NOT NULL AND TRIM(place) <> ''
            ORDER BY id ASC
            """
        )
        rows = [dict(r) for r in cur.fetchall()]
        return rows
    finally:
        conn.close()


def get_incidentes_geocodificados(
    *,
    pais: Optional[str] = None,
    categorias: Optional[List[str]] = None,
    include_without_coords: bool = False,
    start: Optional[str | datetime] = None,
    end: Optional[str | datetime] = None,
    limit: Optional[int] = None,
    order_desc: bool = True,
) -> List[Dict[str, Any]]:
    """
    Recupera incidentes aplicando filtros opcionales.
    Por defecto solo devuelve incidentes con lat/lon definidos.
    """
    conn = _connect()
    try:
        cur = conn.cursor()
        where = []
        params: List[Any] = []

        if not include_without_coords:
            where.append("lat IS NOT NULL AND lon IS NOT NULL")

        if pais:
            where.append("LOWER(pais) = LOWER(?)")
            params.append(pais.strip())

        if categorias:
            cats = [c.strip() for c in categorias if c and c.strip()]
            if cats:
                placeholders = ",".join(["?"] * len(cats))
                where.append(f"categoria IN ({placeholders})")
                params.extend(cats)

        def _normalize_dt(value: str | datetime) -> str:
            if isinstance(value, datetime):
                # usar isoformat con segundos para ser compatible con created_at
                return value.replace(microsecond=0).isoformat()
            return value

        if start:
            where.append("datetime(created_at) >= datetime(?)")
            params.append(_normalize_dt(start))
        if end:
            where.append("datetime(created_at) <= datetime(?)")
            params.append(_normalize_dt(end))

        where_clause = ""
        if where:
            where_clause = "WHERE " + " AND ".join(where)

        order = "created_at DESC" if order_desc else "created_at ASC"
        limit_clause = f" LIMIT {int(limit)}" if limit else ""

        sql = f"""
            SELECT *
            FROM incidentes
            {where_clause}
            ORDER BY {order}
            {limit_clause}
        """
        cur.execute(sql, params)
        rows = [dict(r) for r in cur.fetchall()]
        return rows
    finally:
        conn.close()


def get_incidentes(
    *,
    pais: Optional[str] = None,
    categorias: Optional[List[str]] = None,
    include_without_coords: bool = False,
    start: Optional[str | datetime] = None,
    end: Optional[str | datetime] = None,
    limit: Optional[int] = None,
    order_desc: bool = True,
) -> List[Dict[str, Any]]:
    """
    Compatibilidad con API heredada: delega en get_incidentes_geocodificados.
    """
    return get_incidentes_geocodificados(
        pais=pais,
        categorias=categorias,
        include_without_coords=include_without_coords,
        start=start,
        end=end,
        limit=limit,
        order_desc=order_desc,
    )


def update_incidente_geocode(
    incidente_id: int,
    lat: float,
    lon: float,
    admin1: Optional[str],
    admin2: Optional[str],
    accuracy: Optional[str],
    source: str,
) -> None:
    now = datetime.utcnow().isoformat(timespec="seconds")
    def _op():
        conn = _connect()
        try:
            cur = conn.cursor()
            cur.execute(
                """
                UPDATE incidentes
                SET lat = ?, lon = ?, admin1 = ?, admin2 = ?, accuracy = ?, geocode_source = ?, updated_at = ?
                WHERE id = ?
                """,
                (lat, lon, admin1, admin2, accuracy, source, now, incidente_id),
            )
            conn.commit()
        finally:
            conn.close()

    _retry_locked(_op)


# ---- Geocache (usada por geocoder.py) ----
def geocache_get(key: str) -> Optional[Tuple[float, float, Optional[str], Optional[str], Optional[str], Optional[str]]]:
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute("SELECT lat, lon, country, admin1, admin2, accuracy FROM geocache WHERE key = ?", (key,))
        row = cur.fetchone()
        if not row:
            return None
        return (row[0], row[1], row[2], row[3], row[4], row[5])
    finally:
        conn.close()


def geocache_put(
    key: str,
    lat: float,
    lon: float,
    country: Optional[str],
    admin1: Optional[str],
    admin2: Optional[str],
    accuracy: Optional[str],
    *,
    source: str,
) -> None:
    now = datetime.utcnow().isoformat(timespec="seconds")
    def _op():
        conn = _connect()
        try:
            cur = conn.cursor()
            cur.execute(
                """
                INSERT INTO geocache (key, lat, lon, country, admin1, admin2, accuracy, source, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(key) DO UPDATE SET
                    lat=excluded.lat,
                    lon=excluded.lon,
                    country=excluded.country,
                    admin1=excluded.admin1,
                    admin2=excluded.admin2,
                    accuracy=excluded.accuracy,
                    source=excluded.source,
                    updated_at=excluded.updated_at
                """,
                (key, lat, lon, country, admin1, admin2, accuracy, source, now),
            )
            conn.commit()
        finally:
            conn.close()

    _retry_locked(_op)


# ---- Utilidad opcional: registrar y resolver en una llamada (sin imports circulares) ----
def registrar_incidente_desde_informe(
    *,
    pais: str,
    categoria: str,
    descripcion: str,
    fuente: str,
    lat: float | None = None,
    lon: float | None = None,
    place: str | None = None,
    resolver_ahora: bool = True,
    country_hint: str | None = None,
) -> int:
    init_db(); migrate_db()
    inc_id = add_incidente(
        pais=pais,
        categoria=categoria,
        descripcion=descripcion,
        fuente=fuente,
        lat=lat,
        lon=lon,
        place=place,
    )
    if resolver_ahora:
        try:
            from .incidentes_resolver import resolve_missing_coords  # import diferido
            resolve_missing_coords(default_country_hint=country_hint or pais)
        except Exception:
            pass
    return inc_id
