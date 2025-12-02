try:
    from pydantic import BaseModel
except Exception:  # pragma: no cover - fallback si faltan deps en análisis estático
    BaseModel = object

from dotenv import load_dotenv
import os
from pathlib import Path

load_dotenv()

if BaseModel is object:  # pydantic no disponible -> fallback simple
    from dataclasses import dataclass, field
    from typing import Optional

    @dataclass
    class Settings:
        telegram_bot_token: str
        tz: str = "Africa/Tripoli"
        data_dir: str = "./data"
        default_countries: list[str] = field(default_factory=list)
        # Vuelos
        tequila_api_key: Optional[str] = None
        currency: str = "EUR"
        amadeus_client_id: Optional[str] = None
        amadeus_client_secret: Optional[str] = None
        amadeus_env: str = "test"  # test | prod

        telethon_api_id: Optional[str] = None
        telethon_api_hash: Optional[str] = None
        telethon_session: Optional[str] = None
else:
    class Settings(BaseModel):
        telegram_bot_token: str
        tz: str = "Africa/Tripoli"
        data_dir: str = "./data"
        default_countries: list[str] = []
        # Vuelos
        tequila_api_key: str | None = None
        currency: str = "EUR"
        amadeus_client_id: str | None = None
        amadeus_client_secret: str | None = None
        amadeus_env: str = "test"

        telethon_api_id: str | None = None
        telethon_api_hash: str | None = None
        telethon_session: str | None = None

def get_settings() -> Settings:
    token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    if not token:
        raise RuntimeError("Falta TELEGRAM_BOT_TOKEN en .env")

    tz = os.getenv("TZ", "Africa/Tripoli").strip()
    data_dir = os.getenv("DATA_DIR", "./data").strip()
    default_countries = [s.strip() for s in os.getenv("DEFAULT_COUNTRIES", "").split(",") if s.strip()]

    # Crear carpetas necesarias
    Path(data_dir).mkdir(parents=True, exist_ok=True)
    Path(".session").mkdir(parents=True, exist_ok=True)

    return Settings(
        telegram_bot_token=token,
        tz=tz,
        data_dir=data_dir,
        default_countries=default_countries,
        tequila_api_key=os.getenv("TEQUILA_API_KEY") or None,
        currency=os.getenv("CURRENCY", "EUR"),
        amadeus_client_id=os.getenv("AMADEUS_CLIENT_ID") or None,
        amadeus_client_secret=os.getenv("AMADEUS_CLIENT_SECRET") or None,
        amadeus_env=os.getenv("AMADEUS_ENV", "test"),
        telethon_api_id=os.getenv("TELETHON_API_ID") or None,
        telethon_api_hash=os.getenv("TELETHON_API_HASH") or None,
        telethon_session=os.getenv("TELETHON_SESSION") or ".session/mibot.session",
    )