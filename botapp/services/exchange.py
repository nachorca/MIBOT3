# botapp/services/exchange.py
from __future__ import annotations

import logging
from typing import Dict, Iterable
import requests

logger = logging.getLogger(__name__)

EXCHANGE_API_URL = "https://api.exchangerate.host/latest"


def get_rates(base: str, symbols: Iterable[str]) -> Dict[str, float]:
    """
    Obtiene tasas de cambio desde exchangerate.host.
    base -> divisa base (ej: USD)
    symbols -> lista de divisas objetivo (ej: ["HTG", "EUR"])
    """
    symbols_str = ",".join(symbols)
    try:
        resp = requests.get(
            EXCHANGE_API_URL,
            params={"base": base, "symbols": symbols_str},
            timeout=10,
        )
        resp.raise_for_status()
        data = resp.json()
        rates = data.get("rates", {}) or {}
        return {sym: float(rates.get(sym, 0)) for sym in symbols}
    except Exception as exc:
        logger.error("Error obteniendo tipos de cambio: %s", exc)
        raise


def build_exchange_block(
    local_currency: str,
    local_label: str,
    foreign_currencies: Iterable[str] = ("USD", "EUR"),
) -> str:
    """
    Construye el bloque Exchange del informe SICU.

    Ejemplo Haití:
      build_exchange_block("HTG", "Gourde Haitiano")
    """
    lines = [f"💱 TIPO DE CAMBIO – {local_label} ({local_currency})\n"]

    values: Dict[str, str] = {}

    try:
        # Para cada divisa extranjera, queremos: 1 FOREIGN = X LOCAL
        for foreign in foreign_currencies:
            rates = get_rates(base=foreign, symbols=[local_currency])
            local_val = rates.get(local_currency)
            if local_val and local_val > 0:
                values[foreign] = f"{local_val:.1f} {local_currency}"
            else:
                values[foreign] = f"XXX {local_currency}"
    except Exception:
        # Si falla la API, dejamos valores genéricos
        for foreign in foreign_currencies:
            values[foreign] = f"XXX {local_currency}"

    for foreign in foreign_currencies:
        lines.append(f"• 1 {foreign} = {values[foreign]}")

    lines.append("")
    lines.append("Impacto operativo:")
    lines.append("– Variación de precios en combustible, transportes, logística.")
    lines.append("– Riesgo inflacionario para operaciones prolongadas.")

    return "\n".join(lines)


# 🔁 COMPATIBILIDAD HACIA ATRÁS
# Algunos módulos (exchange_header.py) siguen importando get_exchange_block.
# Definimos un wrapper compatible que delega en build_exchange_block.
def get_exchange_block(
    local_currency: str = "HTG",
    local_label: str = "Gourde Haitiano",
    foreign_currencies: Iterable[str] = ("USD", "EUR"),
) -> str:
    """
    Wrapper de compatibilidad para código antiguo.

    Si se llama sin parámetros, por defecto construye el bloque Exchange
    para Haití (HTG, Gourde Haitiano). Si se llama con parámetros,
    se comporta como build_exchange_block.
    """
    return build_exchange_block(
        local_currency=local_currency,
        local_label=local_label,
        foreign_currencies=foreign_currencies,
    )