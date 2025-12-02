from __future__ import annotations
from pathlib import Path
import re
from ..config import get_settings
from ..utils.time import today_str
from ..services.exchange import get_exchange_block

SET = get_settings()

EX_START = r"^=== EXCHANGE .* ===$"
EX_END = r"^=== FIN EXCHANGE ===$"

def _today_file(country: str) -> Path:
    d = Path(SET.data_dir) / country.lower()
    d.mkdir(parents=True, exist_ok=True)
    return d / f"{today_str(SET.tz)}.txt"

def _has_exchange_block(text: str) -> bool:
    start = re.search(EX_START, text, flags=re.MULTILINE)
    end = re.search(EX_END, text, flags=re.MULTILINE)
    return bool(start and end and start.start() < end.start())

async def prepend_exchange_header(country: str) -> Path:
    f = _today_file(country)
    if not f.exists():
        f.write_text("", encoding="utf-8")
    content = f.read_text(encoding="utf-8")
    if _has_exchange_block(content):
        return f
    block = await get_exchange_block(country)
    updated = (block + content) if content else block
    f.write_text(updated, encoding="utf-8")
    return f