# botapp/handlers/channels.py
from telegram import Update
from telegram.ext import ContextTypes
from urllib.parse import urlparse

from ..config import get_settings
from ..services.channel_registry import ChannelRegistry
from .collect import _is_supported_telegram_identifier, _resolve_entity_or_join
from ..services.telethon_client import TelethonClientHolder, TelethonConfig
from telethon.tl.types import User as TLUser, Channel as TLChannel, Chat as TLChat

SET = get_settings()
REG = ChannelRegistry(SET.data_dir)


def _normalize_channel_arg(s: str) -> str:
    """
    Acepta @usuario, t.me/usuario y links privados (t.me/joinchat/... o t.me/+hash).
    - Convierte t.me/usuario -> @usuario
    - Devuelve el link tal cual si es invitación privada
    - Devuelve s sin cambios en otros casos
    """
    s = (s or "").strip()
    if s.startswith("@"):
        return s
    if (
        s.startswith("https://t.me/")
        or s.startswith("http://t.me/")
        or s.startswith("https://telegram.me/")
        or s.startswith("http://telegram.me/")
    ):
        path_full = urlparse(s).path.lstrip("/")
        path = path_full.split("/", 1)[0] if path_full else ""
        if path.startswith("joinchat/") or path.startswith("+"):
            return s  # invitaciones se guardan tal cual
        if path and not path.startswith("+"):
            return f"@{path}"
    return s


async def addchannel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    /addchannel <pais> <@canal|t.me/usuario|t.me/joinchat/...|t.me/+hash>
    Normaliza el identificador y lo guarda en el registro por país
    (data/sources_telegram.json).
    """
    if len(context.args) < 2:
        return await update.message.reply_text("Uso: /addchannel <pais> <@canal|t.me/…>")

    country = context.args[0].lower().strip()
    ch_raw = context.args[1].strip()
    channel = _normalize_channel_arg(ch_raw)

    ok = REG.add(country, channel)
    msg = "✅ Añadido" if ok else "ℹ️ Ya estaba"
    await update.message.reply_text(f"{msg}: {channel}")


async def delchannel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    /delchannel <pais> <@canal|t.me/...>
    Elimina un canal del registro (sources_telegram.json).
    """
    if len(context.args) < 2:
        return await update.message.reply_text("Uso: /delchannel <pais> <@canal>")
    country = context.args[0].lower().strip()
    channel = context.args[1].strip()
    ok = REG.remove(country, channel)
    await update.message.reply_text("🗑️ Eliminado" if ok else "❌ No estaba")


async def listchannels(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    /listchannels [pais]

    Lista los canales de Telegram configurados en data/sources_telegram.json.
    """
    if context.args:
        country = context.args[0].lower().strip()
        chans = REG.list_channels(country)
        if not chans:
            return await update.message.reply_text(f"No hay canales para {country}.")
        return await update.message.reply_text(f"Canales {country}:\n" + "\n".join(chans))
    # todos
    text = []
    for c in REG.list_countries():
        chans = REG.list_channels(c)
        text.append(f"• {c}: " + (", ".join(chans) if chans else "—"))
    await update.message.reply_text("\n".join(text) if text else "No hay países/canales.")


async def checkchannels(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    /checkchannels [pais]

    Diagnostica cada entrada de data/sources_telegram.json con la cuenta Telethon e informa estado y motivo.
    """
    holder = TelethonClientHolder(
        TelethonConfig(SET.telethon_api_id, SET.telethon_api_hash, SET.telethon_session)
    )
    client = await holder.get_client()
    if client is None:
        return await update.message.reply_text(
            "Telethon no está configurado o no hay sesión autorizada. "
            "Ejecuta tools/telethon_login.py y reintenta."
        )

    countries = [context.args[0].lower().strip()] if context.args else REG.list_countries()
    lines = []
    for country in countries:
        chans = REG.list_channels(country)
        if not chans:
            lines.append(f"• {country}: —")
            continue
        lines.append(f"• {country}:")
        for ch in chans:
            if not _is_supported_telegram_identifier(ch):
                lines.append(f"  - {ch}: NO-TELEGRAM (debería ir en fuentes web/X, no en sources_telegram.json)")
                continue
            entity, reason = await _resolve_entity_or_join(client, ch)
            if entity is None:
                lines.append(f"  - {ch}: NO-RESUELTO ({reason or 'desconocido'})")
                continue
            if isinstance(entity, TLUser):
                lines.append(f"  - {ch}: USUARIO (no canal/grupo)")
            elif isinstance(entity, (TLChannel, TLChat)):
                title = getattr(entity, "title", None) or "canal/grupo"
                lines.append(f"  - {ch}: OK ({title})")
            else:
                lines.append(f"  - {ch}: TIPO-DESCONOCIDO")

    # Evitar mensajes demasiado largos: enviar en bloques de 50 líneas
    chunk = []
    count = 0
    for ln in lines:
        chunk.append(ln)
        count += 1
        if count % 50 == 0:
            await update.message.reply_text("\n".join(chunk))
            chunk = []
    if chunk:
        await update.message.reply_text("\n".join(chunk))