from __future__ import annotations
import argparse
from pathlib import Path
from typing import List
import re

# Patrones locales (idénticos a los usados en el scraper) para detectar banners/mensajes de PWA en iOS/iPad.
NOISE_PATTERNS: List[re.Pattern[str]] = [
    re.compile(r"install\s+pwa\s+using\s+add\s+to\s+home\s+screen", re.IGNORECASE),
    re.compile(r"for\s+ios\s+and\s+ipad\s+browsers.*add\s+to\s+(home\s+screen|dock)", re.IGNORECASE),
    re.compile(r"add\s+to\s+home\s+screen\s+in\s+ios\s+safari", re.IGNORECASE),
]


def remove_noise_lines(text: str) -> str:
    if not text:
        return text
    out_lines: List[str] = []
    for ln in text.splitlines():
        lns = ln.strip()
        # si la línea coincide con alguno de los patrones, la omitimos
        if any(p.search(lns) for p in NOISE_PATTERNS):
            continue
        out_lines.append(ln)
    return "\n".join(out_lines)


def main(data_dir: Path, dry_run: bool = False) -> int:
    changed = 0
    for path in data_dir.rglob("*.txt"):
        try:
            before = path.read_text(encoding="utf-8")
        except Exception:
            continue
        after = remove_noise_lines(before)
        if after != before:
            changed += 1
            if not dry_run:
                path.write_text(after, encoding="utf-8")
            print(f"cleaned: {path}")
    print(f"done. files changed: {changed}")
    return changed


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Remove known noise/footer lines (e.g., iOS PWA banners) from scraped .txt files under data/.")
    parser.add_argument("data_dir", nargs="?", default="data", help="Path to the data directory (default: data)")
    parser.add_argument("--dry-run", action="store_true", help="Only report files that would be changed")
    args = parser.parse_args()

    data_path = Path(args.data_dir).resolve()
    if not data_path.exists():
        raise SystemExit(f"data directory not found: {data_path}")
    main(data_path, dry_run=bool(args.dry_run))
