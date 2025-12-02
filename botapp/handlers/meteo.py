from telegram import Update
from telegram.ext import ContextTypes
from ..utils.meteo_header import prepend_weather_header

async def meteo(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    /meteo <pais>
    Refresca/añade el bloque METEO y responde con el TXT actualizado.
    """
    if not context.args:
        return await update.message.reply_text("Uso: /meteo <pais>")
    country = context.args[0].lower().strip()
    f = await prepend_weather_header(country)
    await update.message.reply_document(document=f.open("rb"), filename=f.name, caption="METEO actualizada")