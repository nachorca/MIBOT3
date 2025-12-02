from pathlib import Path
from typing import Iterable
from . import __init__  # noqa: F401  (para paquetes)
import os
import re
from datetime import datetime

# Cabecera estándar de tus entradas:
# --- @canal @ YYYY-MM-DD HH:MM:SS ---
HEADER_RE = re.compile(
    r"^---\s*(?P<title>.+?)\s*@\s*(?P<dt>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\s*---\s*$",
    re.MULTILINE
)

def _parse_blocks_by_header(text: str):
    """
    Devuelve (prefix, entries) donde entries = lista de dicts con:
    {'start': int, 'end': int, 'title': str, 'dt': datetime, 'content': str}
    prefix = texto antes del primer header (p.ej. METEO / EXCHANGE)
    """
    entries = []
    matches = list(HEADER_RE.finditer(text))
    if not matches:
        return text, []  # todo es prefijo (no hay entradas)

    prefix = text[:matches[0].start()]
    for i, m in enumerate(matches):
        start = m.start()
        end = matches[i+1].start() if i + 1 < len(matches) else len(text)
        chunk = text[start:end]
        title = m.group("title").strip()
        dt = datetime.strptime(m.group("dt"), "%Y-%m-%d %H:%M:%S")
        entries.append({
            "start": start,
            "end": end,
            "title": title,
            "dt": dt,
            "content": chunk,
        })
    return prefix, entries

class Store:
    """
    Pequeña capa de persistencia en TXT por país y por día.
    data/{pais}/YYYY-MM-DD.txt
    """
    def __init__(self, data_dir: str):
        self.base = Path(data_dir)
        self.base.mkdir(parents=True, exist_ok=True)

    def _country_dir(self, country: str) -> Path:
        d = self.base / country.lower()
        d.mkdir(parents=True, exist_ok=True)
        return d

    def append_entry(self, country: str, day: str, title: str, dt: str, text: str) -> Path:
        f = self._country_dir(country) / f"{day}.txt"
        with f.open("a", encoding="utf-8") as fh:
            fh.write(f"--- {title} @ {dt} ---\n{text.strip()}\n\n")
        return f

    def read_recent(self, country: str, days_files: Iterable[str]) -> str:
        buf = []
        for day in days_files:
            f = self._country_dir(country) / f"{day}.txt"
            if f.exists():
                buf.append(f"\n===== {country.upper()} :: {day} =====\n")
                buf.append(f.read_text(encoding="utf-8"))
        return "".join(buf)

    def latest_file(self, country: str) -> Path | None:
        d = self._country_dir(country)
        files = sorted([p for p in d.glob("*.txt") if p.is_file()], reverse=True)
        return files[0] if files else None

    def reorder_file(self, file_path: Path) -> Path:
        """
        Reordena IN-PLACE las entradas del TXT:
        1) Mantiene intacto el prefijo (METEO, EXCHANGE, etc.)
        2) Ordena los bloques por hora de entrada (asc) y por canal (asc).
        """
        if not file_path.exists():
            return file_path

        text = file_path.read_text(encoding="utf-8")
        prefix, entries = _parse_blocks_by_header(text)

        if not entries:
            # Nada que ordenar
            return file_path

        # Orden: primero por fecha/hora ascendente, luego por nombre de canal/título ascendente.
        entries.sort(key=lambda e: (e["dt"], e["title"].lower()))

        new_text = prefix + "".join(e["content"] for e in entries)
        file_path.write_text(new_text, encoding="utf-8")
        return file_path