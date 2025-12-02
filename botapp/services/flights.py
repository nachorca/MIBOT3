from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import List, Optional, Dict, Any
import os
import aiohttp
from urllib.parse import urlencode

# ===== Modelos de dominio =====
@dataclass
class Flight:
    carrier: str
    flight_number: str
    depart_airport: str
    arrive_airport: str
    depart_dt: datetime
    arrive_dt: datetime
    price: float
    currency: str
    stops: int

    @property
    def duration(self) -> timedelta:
        return self.arrive_dt - self.depart_dt

@dataclass
class Itinerary:
    out_flight: Flight
    in_flight: Optional[Flight]  # None si es solo ida
    booking_url: Optional[str] = None

    @property
    def total_price(self) -> float:
        return self.out_flight.price + (self.in_flight.price if self.in_flight else 0.0)

    @property
    def currency(self) -> str:
        return self.out_flight.currency

    @property
    def final_arrival(self) -> datetime:
        # Para priorizar “tiempo de llegada a destino”:
        return (self.in_flight.arrive_dt if self.in_flight else self.out_flight.arrive_dt)

# ===== Generadores de enlaces de reserva/agregadores =====
def build_google_flights_link(origin: str, dest: str, depart_iso: str, return_iso: Optional[str] = None) -> str:
    base = "https://www.google.com/travel/flights?hl=es#flt="
    outp = f"{origin}.{dest}.{depart_iso}"
    if return_iso:
        return f"{base}{outp}*{dest}.{origin}.{return_iso}"
    return f"{base}{outp}"

def build_skyscanner_link(origin: str, dest: str, depart_iso: str, return_iso: Optional[str] = None, *, adults: int = 1, cabin: str = "economy", currency: str = "EUR", locale: str = "es-ES") -> str:
    # Skyscanner usa fechas YYMMDD en la ruta
    d1 = datetime.fromisoformat(depart_iso).strftime("%y%m%d")
    base = "https://www.skyscanner.es/transport/flights"
    query = f"adults={adults}&cabinclass={cabin}&preferdirects=false&currency={currency}&locale={locale}"
    if return_iso:
        d2 = datetime.fromisoformat(return_iso).strftime("%y%m%d")
        return f"{base}/{origin}/{dest}/{d1}/{d2}/?{query}"
    return f"{base}/{origin}/{dest}/{d1}/?{query}"

@dataclass
class SearchParams:
    origin: str      # IATA (p.ej., TUN, MJI, MAD)
    destination: str
    depart_date: str # YYYY-MM-DD
    return_date: Optional[str] = None  # None => solo ida
    preference: str = "economico"      # "economico" | "rapido"
    provider: Optional[str] = None      # "tequila" | "amadeus" | "dummy"

# ===== Proveedor base =====
class FlightsProvider:
    async def search(self, p: SearchParams) -> List[Itinerary]:
        raise NotImplementedError

# ===== Proveedor dummy (funciona sin API) =====
class DummyProvider(FlightsProvider):
    async def search(self, p: SearchParams) -> List[Itinerary]:
        # Generamos resultados deterministas básicos
        # Nota: Solo para demo; integra Amadeus/Skyscanner aquí luego.
        base_dt = datetime.fromisoformat(p.depart_date + "T08:00:00")
        cur = "EUR"
        out_direct = Flight("TU", "754", p.origin, p.destination, base_dt, base_dt + timedelta(hours=2, minutes=25), 120.0, cur, 0)
        out_1stop = Flight("AF", "1283", p.origin, p.destination, base_dt + timedelta(hours=1), base_dt + timedelta(hours=5, minutes=10), 95.0, cur, 1)

        itineraries: List[Itinerary] = []

        if p.return_date:
            ret_dt = datetime.fromisoformat(p.return_date + "T17:30:00")
            in_direct = Flight("IB", "3721", p.destination, p.origin, ret_dt, ret_dt + timedelta(hours=2, minutes=20), 110.0, cur, 0)
            in_1stop = Flight("VY", "8712", p.destination, p.origin, ret_dt + timedelta(hours=1), ret_dt + timedelta(hours=6), 85.0, cur, 1)
            link = build_skyscanner_link(out_direct.depart_airport, out_direct.arrive_airport, p.depart_date, p.return_date, currency=cur)
            itineraries.extend([
                Itinerary(out_direct, in_direct, booking_url=link),
                Itinerary(out_direct, in_1stop, booking_url=link),
                Itinerary(out_1stop, in_direct, booking_url=link),
                Itinerary(out_1stop, in_1stop, booking_url=link),
            ])
        else:
            link = build_skyscanner_link(out_direct.depart_airport, out_direct.arrive_airport, p.depart_date, None, currency=cur)
            itineraries.extend([
                Itinerary(out_direct, None, booking_url=link),
                Itinerary(out_1stop, None, booking_url=link),
            ])
        return itineraries

# ===== Proveedor real: Kiwi Tequila API =====
class KiwiProvider(FlightsProvider):
    BASE_URL = "https://api.tequila.kiwi.com/v2/search"

    def __init__(self, api_key: str, currency: str = "EUR") -> None:
        self.api_key = api_key
        self.currency = currency

    async def search(self, p: SearchParams) -> List[Itinerary]:
        # Tequila usa formato dd/mm/YYYY
        def _dmy(date_str: str) -> str:
            return datetime.fromisoformat(date_str).strftime("%d/%m/%Y")

        params = {
            "fly_from": p.origin,
            "fly_to": p.destination,
            "date_from": _dmy(p.depart_date),
            "date_to": _dmy(p.depart_date),
            "curr": self.currency,
            "limit": 10,
            "adults": 1,
            "sort": "price" if p.preference != "rapido" else "duration",
        }
        if p.return_date:
            params.update({
                "return_from": _dmy(p.return_date),
                "return_to": _dmy(p.return_date),
            })

        headers = {"apikey": self.api_key}
        async with aiohttp.ClientSession(headers=headers) as session:
            url = f"{self.BASE_URL}?{urlencode(params)}"
            async with session.get(url, timeout=30) as r:
                r.raise_for_status()
                data = await r.json()
        items = []
        for it in data.get("data", []):
            # Parsers simplificados: out/in de first/last segments
            route = it.get("route", [])
            out_seg = [s for s in route if s.get("return") == 0]
            in_seg = [s for s in route if s.get("return") == 1]

            def build_flight(segments: List[Dict[str, Any]]) -> Optional[Flight]:
                if not segments:
                    return None
                first = segments[0]
                last = segments[-1]
                carrier = first.get("airline") or (first.get("operating_carrier") or "")
                flight_no = f"{carrier}{first.get('flight_no', '')}"
                depart_airport = first.get("flyFrom", "")
                arrive_airport = last.get("flyTo", "")
                depart_dt = datetime.fromisoformat(first.get("local_departure").replace("Z", "+00:00"))
                arrive_dt = datetime.fromisoformat(last.get("local_arrival").replace("Z", "+00:00"))
                stops = max(0, len(segments) - 1)
                return Flight(
                    carrier=carrier,
                    flight_number=flight_no.replace(carrier, ""),
                    depart_airport=depart_airport,
                    arrive_airport=arrive_airport,
                    depart_dt=depart_dt,
                    arrive_dt=arrive_dt,
                    price=float(it.get("price", 0.0)),
                    currency=self.currency,
                    stops=stops,
                )

            out_f = build_flight(out_seg)
            in_f = build_flight(in_seg)
            if out_f is None:
                continue
            items.append(Itinerary(out_f, in_f, booking_url=it.get("deep_link") or None))
        return items

# ===== Proveedor real: Amadeus Flight Offers =====
class AmadeusProvider(FlightsProvider):
    def __init__(self, client_id: str, client_secret: str, env: str = "test", currency: str = "EUR") -> None:
        self.client_id = client_id
        self.client_secret = client_secret
        self.currency = currency
        base = "https://test.api.amadeus.com" if env != "prod" else "https://api.amadeus.com"
        self.token_url = f"{base}/v1/security/oauth2/token"
        self.search_url = f"{base}/v2/shopping/flight-offers"

    async def _get_token(self, session: aiohttp.ClientSession) -> str:
        data = {
            "grant_type": "client_credentials",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
        }
        async with session.post(self.token_url, data=data, timeout=30) as r:
            r.raise_for_status()
            js = await r.json()
            return js["access_token"]

    async def search(self, p: SearchParams) -> List[Itinerary]:
        # Amadeus usa JSON POST; currency va en "currencyCode"
        async with aiohttp.ClientSession() as session:
            token = await self._get_token(session)
            headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
            payload: Dict[str, Any] = {
                "currencyCode": self.currency,
                "originLocationCode": p.origin,
                "destinationLocationCode": p.destination,
                "departureDate": p.depart_date,
                "adults": 1,
                "max": 10,
                "nonStop": False,
            }
            if p.return_date:
                payload["returnDate"] = p.return_date

            async with session.get(self.search_url, headers=headers, params=payload, timeout=30) as r:
                r.raise_for_status()
                data = await r.json()

        items: List[Itinerary] = []
        for offer in data.get("data", []):
            itineraries = offer.get("itineraries", [])
            price = float(offer.get("price", {}).get("grandTotal", 0.0))
            cur = offer.get("price", {}).get("currency", self.currency)

            def parse_bound(bound: Dict[str, Any]) -> Optional[Flight]:
                segs = bound.get("segments", [])
                if not segs:
                    return None
                first = segs[0]
                last = segs[-1]
                dep = first.get("departure", {})
                arr = last.get("arrival", {})
                carrier = first.get("carrierCode", "")
                flight_no = f"{carrier}{first.get('number', '')}"
                depart_airport = dep.get("iataCode", "")
                arrive_airport = arr.get("iataCode", "")
                depart_dt = datetime.fromisoformat(dep.get("at").replace("Z", "+00:00"))
                arrive_dt = datetime.fromisoformat(arr.get("at").replace("Z", "+00:00"))
                stops = max(0, len(segs) - 1)
                return Flight(
                    carrier=carrier,
                    flight_number=flight_no.replace(carrier, ""),
                    depart_airport=depart_airport,
                    arrive_airport=arrive_airport,
                    depart_dt=depart_dt,
                    arrive_dt=arrive_dt,
                    price=price,  # Nota: en Amadeus el precio es por oferta, lo aplicamos al out_flight
                    currency=cur,
                    stops=stops,
                )

            out_f = parse_bound(itineraries[0]) if len(itineraries) >= 1 else None
            in_f = parse_bound(itineraries[1]) if len(itineraries) >= 2 else None
            if out_f is None:
                continue
            # Amadeus no da deeplink de reserva directa: generamos enlace a Skyscanner (muestra botón de reservar)
            skl = build_skyscanner_link(p.origin, p.destination, p.depart_date, p.return_date, currency=cur)
            items.append(Itinerary(out_f, in_f, booking_url=skl))
        return items

# ===== Orquestador + ordenación por preferencia =====

IATA_ALIASES = {
    # ciudades → IATA
    "tunez": "TUN", "tunis": "TUN",
    "tripoli": "MJI",  # Mitiga (operativo). TIP (Tripoli Intl) está cerrado.
    "madrid": "MAD",
}

def resolve_iata(city_or_iata: str) -> str:
    s = city_or_iata.strip().lower()
    return IATA_ALIASES.get(s, city_or_iata.upper())

def sort_itineraries(items: List[Itinerary], preference: str) -> List[Itinerary]:
    if preference == "rapido":
        # Rapidez primero (llegada final), después precio
        return sorted(items, key=lambda it: (it.final_arrival, it.total_price, it.out_flight.stops + (it.in_flight.stops if it.in_flight else 0)))
    # Por defecto: económico, luego llegada final
    return sorted(items, key=lambda it: (it.total_price, it.final_arrival, it.out_flight.stops + (it.in_flight.stops if it.in_flight else 0)))

class FlightsService:
    def __init__(self) -> None:
        # Selecciona proveedor (futuro: Amadeus, Skyscanner, Duffel, etc.)
        # Usa AMADEUS_API_KEY si quieres conmutar en el futuro.
        from ..config import get_settings
        s = get_settings()
        if s.tequila_api_key:
            self.provider = KiwiProvider(s.tequila_api_key, currency=s.currency)
        elif s.amadeus_client_id and s.amadeus_client_secret:
            self.provider = AmadeusProvider(s.amadeus_client_id, s.amadeus_client_secret, env=s.amadeus_env, currency=s.currency)
        else:
            self.provider = DummyProvider()
        self._providers_cache = {
            "tequila": KiwiProvider(s.tequila_api_key, currency=s.currency) if s.tequila_api_key else None,
            "amadeus": AmadeusProvider(s.amadeus_client_id, s.amadeus_client_secret, env=s.amadeus_env, currency=s.currency) if (s.amadeus_client_id and s.amadeus_client_secret) else None,
            "dummy": DummyProvider(),
        }

    async def search(self, p: SearchParams) -> List[Itinerary]:
        # Normaliza IATA
        p.origin = resolve_iata(p.origin)
        p.destination = resolve_iata(p.destination)
        # Selección de proveedor por búsqueda si se solicita
        prov = self.provider
        if p.provider:
            key = p.provider.strip().lower()
            sel = self._providers_cache.get(key)
            if sel is None:
                # Si no está disponible (p.ej. falta API key), caemos al por defecto
                sel = prov
            prov = sel
        # Busca y ordena
        results = await prov.search(p)
        return sort_itineraries(results, p.preference)