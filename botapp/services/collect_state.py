from __future__ import annotations
from pathlib import Path
import json
from typing import Dict

class CollectState:
    """
    Guarda last_message_id por canal:
    { "@canal": 123456 }
    """
    def __init__(self, data_dir: str):
        self.path = Path(data_dir) / "collect_state.json"
        self.path.parent.mkdir(parents=True, exist_ok=True)
        if not self.path.exists():
            self._write({})
        self._cache = self._read()

    def _read(self) -> Dict[str, int]:
        try:
            return json.loads(self.path.read_text(encoding="utf-8"))
        except Exception:
            return {}

    def _write(self, data: Dict[str, int]) -> None:
        self.path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

    def get_last_id(self, channel: str) -> int:
        return int(self._cache.get(channel, 0))

    def set_last_id(self, channel: str, msg_id: int) -> None:
        self._cache[channel] = int(msg_id)
        self._write(self._cache)