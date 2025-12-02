from datetime import datetime
import pytz

def now_tz(tz_name: str) -> datetime:
    tz = pytz.timezone(tz_name)
    return datetime.now(tz)

def today_str(tz_name: str) -> str:
    return now_tz(tz_name).strftime("%Y-%m-%d")

def dt_str(tz_name: str) -> str:
    return now_tz(tz_name).strftime("%Y-%m-%d %H:%M:%S")


# --- Bloque de prueba ---
if __name__ == "__main__":
    tz = "Africa/Tripoli"  # aquí puedes poner la zona horaria que quieras
    print("Ahora:", now_tz(tz))
    print("Hoy:", today_str(tz))
    print("Fecha completa:", dt_str(tz))