from __future__ import annotations
from dataclasses import dataclass
from typing import Optional
from telethon import TelegramClient

@dataclass
class TelethonConfig:
    api_id: Optional[int]
    api_hash: Optional[str]
    session_path: Optional[str]

class TelethonClientHolder:
    def __init__(self, cfg: TelethonConfig):
        self.cfg = cfg
        self._client: Optional[TelegramClient] = None

    async def get_client(self) -> Optional[TelegramClient]:
        """
        Devuelve un TelegramClient conectado y autorizado, o None si falta config
        o no hay sesión. NO intenta iniciar sesión desde aquí (eso se hace con el script tools/telethon_login.py).
        """
        if not (self.cfg.api_id and self.cfg.api_hash and self.cfg.session_path):
            return None

        if self._client is None:
            self._client = TelegramClient(self.cfg.session_path, int(self.cfg.api_id), self.cfg.api_hash)
            await self._client.connect()

            # Si no está autorizado, devolvemos None y dejamos que el caller lo gestione (no bloqueamos el bot).
            if not await self._client.is_user_authorized():
                # Importante: NO llames send_code_request aquí; el bot no debe pedir input.
                return None

        return self._client