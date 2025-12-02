from __future__ import annotations
import asyncio
import os
from telethon import TelegramClient
from botapp.config import get_settings

async def main():
    s = get_settings()
    api_id = s.telethon_api_id
    api_hash = s.telethon_api_hash
    session = s.telethon_session
    phone = os.getenv("TELETHON_PHONE")
    password = os.getenv("TELETHON_PASSWORD") or None

    if not (api_id and api_hash and session):
        print("❌ Faltan TELETHON_API_ID/TELETHON_API_HASH/TELETHON_SESSION en .env")
        return
    if not phone:
        print("❌ Falta TELETHON_PHONE en .env")
        return

    client = TelegramClient(session, int(api_id), api_hash)
    await client.connect()

    if await client.is_user_authorized():
        me = await client.get_me()
        print(f"✅ Ya autorizado como: {me.username or me.first_name}")
        await client.disconnect()
        return

    print(f"➡️ Iniciando login para {phone} …")
    code = None
    await client.send_code_request(phone)
    code = input("Introduce el código recibido por Telegram/SMS: ").strip()

    try:
        await client.sign_in(phone=phone, code=code)
    except Exception as e:
        # Si tienes 2FA, pedirá contraseña
        if "SESSION_PASSWORD_NEEDED" in str(e).upper():
            if not password:
                password = input("Contraseña (2FA) de Telegram: ").strip()
            await client.sign_in(password=password)
        else:
            raise

    me = await client.get_me()
    print(f"✅ Sesión creada y guardada. Usuario: {me.username or me.first_name}")
    await client.disconnect()

if __name__ == "__main__":
    asyncio.run(main())