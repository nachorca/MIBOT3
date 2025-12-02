# services/sicu_map.py
from __future__ import annotations
import pandas as pd
import folium
from folium.plugins import MarkerCluster
from pathlib import Path

CITY_COORDS = {
    "Tripoli": (32.8872, 13.1913),
    "Benghazi": (32.1167, 20.0667),
    "Zawiya": (32.7571, 12.7278),
    "Ras Ajdair": (33.1020, 11.5462),
    "Sirte": (31.2058, 16.5887),
    "Kufra": (24.2000, 23.3000),
    "Misrata": (32.3754, 15.0925),
    "Ain Zara": (32.8200, 13.2160),
    "Al Marj": (32.4876, 20.8337),
    "Zintan": (31.9310, 12.2523),
    "Sebha": (27.0377, 14.4283),
    "Derna": (32.7670, 22.6390),
    "Al Khums": (32.6475, 14.2619),
    "Zliten": (32.4674, 14.5687),
    "Tobruk": (32.0836, 23.9764),
    "Ghariyan": (32.1722, 13.0209),
}

CAT_COLOR = {
    "Conflicto Armado": "red",
    "Terrorismo": "darkred",
    "Delincuencia": "orange",
    "Disturbios Civiles": "blue",
    "Hazards": "green",
}

def _extract_city(loc_text: str) -> str:
    # "Tripoli (طرابلس)" -> "Tripoli"
    if "(" in loc_text:
        return loc_text.split("(")[0].strip()
    return loc_text.strip()

def build_sicu_map(csv_in: str, html_out: str) -> str:
    """
    Genera un mapa Folium (Leaflet) a partir de un CSV SICU con columnas:
    Fecha, Hora, Localización, Categoría SICU, Breve descripción, Subcategoría, Nivel de severidad
    """
    df = pd.read_csv(csv_in)
    required = {"Fecha","Hora","Localización","Categoría SICU","Breve descripción","Subcategoría","Nivel de severidad"}
    missing = required.difference(df.columns)
    if missing:
        raise ValueError(f"CSV inválido. Faltan columnas: {', '.join(sorted(missing))}")

    m = folium.Map(location=[27.0, 17.0], zoom_start=5)
    cluster = MarkerCluster().add_to(m)

    for _, row in df.iterrows():
        city = _extract_city(str(row["Localización"]))
        coords = CITY_COORDS.get(city, (27.0, 17.0))
        color = CAT_COLOR.get(str(row["Categoría SICU"]), "gray")
        popup_html = f"""
        <b>{row['Categoría SICU']}</b> — <i>{row['Subcategoría']}</i><br>
        <b>Fecha/Hora:</b> {row['Fecha']} {row['Hora']}<br>
        <b>Localización:</b> {row['Localización']}<br>
        <b>Severidad:</b> {row['Nivel de severidad']}<br>
        <div style='margin-top:4px'>{row['Breve descripción']}</div>
        """
        folium.Marker(
            coords,
            popup=folium.Popup(popup_html, max_width=420),
            icon=folium.Icon(color=color, icon="info-sign"),
        ).add_to(cluster)

    legend_html = """
    <div style="position: fixed; bottom: 20px; left: 20px; width: 220px; z-index: 9999; font-size: 14px;
         background-color: white; padding: 10px; border: 2px solid #444; border-radius: 8px;">
    <b>Leyenda SICU</b><br>
    <span style="color:#d9534f;">■</span> Conflicto Armado<br>
    <span style="color:#8B0000;">■</span> Terrorismo<br>
    <span style="color:#f0ad4e;">■</span> Delincuencia<br>
    <span style="color:#0275d8;">■</span> Disturbios Civiles<br>
    <span style="color:#5cb85c;">■</span> Hazards
    </div>
    """
    m.get_root().html.add_child(folium.Element(legend_html))

    out_path = Path(html_out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    m.save(str(out_path))
    return str(out_path)