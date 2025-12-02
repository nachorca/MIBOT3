from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
import json
from typing import Optional, Dict, Any
from telethon.tl.types import InputPeerChannel, InputPeerChat, Channel as TLChannel, Chat as TLChat


@dataclass
class EntityRecord:
    type: str  # "channel" | "chat"
    id: int
    access_hash: Optional[int] = None
    title: Optional[str] = None
    username: Optional[str] = None

    def to_input_peer(self):
        if self.type == "channel" and self.access_hash is not None:
            return InputPeerChannel(self.id, self.access_hash)
        if self.type == "chat":
            return InputPeerChat(self.id)
        return None


class EntityCache:
    def __init__(self, data_dir: str) -> None:
        self.path = Path(data_dir) / "entity_cache.json"
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._cache: Dict[str, EntityRecord] = {}
        self._load()

    def _load(self) -> None:
        try:
            raw = json.loads(self.path.read_text(encoding="utf-8"))
            for k, v in raw.items():
                self._cache[k] = EntityRecord(**v)
        except Exception:
            self._cache = {}

    def _save(self) -> None:
        data: Dict[str, Any] = {k: vars(v) for k, v in self._cache.items()}
        self.path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

    def get_input_peer(self, key: str):
        rec = self._cache.get(key)
        if not rec:
            return None
        return rec.to_input_peer()

    def remember(self, key: str, entity) -> bool:
        try:
            if isinstance(entity, TLChannel):
                rec = EntityRecord(
                    type="channel",
                    id=entity.id,
                    access_hash=getattr(entity, "access_hash", None),
                    title=getattr(entity, "title", None),
                    username=getattr(entity, "username", None),
                )
            elif isinstance(entity, TLChat):
                rec = EntityRecord(
                    type="chat",
                    id=entity.id,
                    access_hash=None,
                    title=getattr(entity, "title", None),
                    username=None,
                )
            else:
                return False
            self._cache[key] = rec
            self._save()
            return True
        except Exception:
            return False
