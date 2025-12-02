# MIBOT3 – Flujo de incidentes SICU

Este repositorio ya incorpora soporte para registrar incidentes operativos en la base de datos `data/incidentes.sqlite3`, clasificarlos según el catálogo SICU y generar un mapa HTML con todos los eventos geolocalizados.

## 1. Preparar los incidentes

1. Crea un archivo `JSON` o `JSONL` con las entradas a importar. Campos mínimos:
   - `pais` (p.ej. `"Libia"`)
   - `categoria` (código SICU que utilices internamente, p.ej. `"SICU-01"`)
   - `descripcion` (texto del hecho)
   - `fuente` (URL o referencia breve)
   - Opcional: `lat`, `lon` (si ya tienes coordenadas), `place` (nombre del barrio/ciudad), `country_hint` (override para geocodificación)

   Ejemplo `data/incidentes/libia.jsonl`:

   ```json
   {"pais": "Libia", "categoria": "SICU-01", "descripcion": "Enfrentamiento armado en el barrio de Gargaresh.", "fuente": "https://ejemplo.com/incidente1", "place": "Gargaresh, Tripoli"}
   {"pais": "Libia", "categoria": "SICU-03", "descripcion": "Explosión controlada cercana al puerto de Misrata.", "fuente": "https://ejemplo.com/incidente2", "place": "Puerto de Misrata"}
   ```

2. Lanza la importación (usa `--no-geocode` si no quieres que intente resolver coordenadas inmediatamente):

   ```bash
   python3 -m botapp.tools.import_incidentes data/incidentes/libia.jsonl --country-hint "Libia"
   ```

   Cada entrada se almacena en la tabla `incidentes`; si `lat/lon` están vacíos pero `place` tiene valor, se intentará geocodificar usando Nominatim (con caché local). El hint de país sirve para incidentes donde solo se conoce el barrio o la ciudad.

## 2. Ajustar el catálogo SICU (colores/iconos)

Los estilos del mapa se cargan desde `data/sicu_catalog.json` (si existe). Formato esperado:

```json
{
  "SICU-01": {"label": "Acciones armadas", "color": "#d73027", "icon": "crosshairs"},
  "SICU-03": {"label": "Explosivos", "color": "#4575b4"}
}
```

Si una categoría no está definida, se generan colores automáticamente a partir de una paleta predefinida.

## 3. Generar el mapa interactivo

1. Ejecuta el generador:

   ```bash
   python3 -m botapp.tools.generate_incident_map --output data/maps/incidentes_libia.html --pais Libia
   ```

   Parámetros útiles:
   - `--categoria` (repetible) para filtrar códigos SICU concretos.
   - `--start` / `--end` en formato ISO (`2025-10-15T07:00:00`) para limitar por fechas.
   - `--tiles` para cambiar el fondo (`OpenStreetMap`, `Stamen Terrain`, etc.).
   - `--no-legend` para ocultar la leyenda.

2. Abre el HTML generado (`data/maps/*.html`) en el navegador. Cada incidente muestra:
   - Categoría y etiqueta SICU
   - Ubicación nominal (`place`), país y precisión de geocodificación
   - Descripción y fuente
   - Fecha de creación/actualización

## 4. Geocodificación y tareas pendientes

- La geocodificación usa Nominatim y respeta un ritmo de 1 petición/s. Si trabajas sin conexión, desactiva `USE_ONLINE_GEOCODER` en `botapp/services/geocoder.py`.
- Para mejorar la calidad de los datos, revisa los incidentes sin `lat/lon` con el comando `/incidentes_resolve` del bot (usa un hint de país si es necesario).
- Si necesitas definir las reglas de clasificación (catálogo SICU) o automatizar la extracción desde los txt diarios, comparte el documento del catálogo para afinar los mapeos y evitar ambigüedades.

## 5. Traducción offline (Argos + MarianMT)

- El bot intenta traducir resúmenes con modelos MarianMT cacheados en `data/hf_models`. Por defecto el runtime se mantiene **offline** para evitar solicitudes a Hugging Face (que provocan errores 401 en entornos sin credenciales).
- Si necesitas que descargue automáticamente modelos nuevos, exporta la variable de entorno `HF_ALLOW_REMOTE_MODELS=1` antes de lanzar el bot. Esto re-habilita las descargas y desactiva el modo offline.
- Independientemente de MarianMT, siempre puedes colocar modelos de Argos Translate (`*.argosmodel`) en `data/argos_models` para ampliar la cobertura sin salir del entorno local.

## 6. Scraping de fuentes web (sources.json)

- El comando `/scrape <país> [max_paginas|full] [min_len] [visit_factor]` acepta ahora `full`, `*` o valores `<=0` para recorrer todo el sitio hasta agotar enlaces. El mismo formato aplica a `/scrape_all`.
- También puedes fijar los valores por defecto vía variables de entorno: `SCRAPE_MAX_PAGES` (0 = sin límite), `SCRAPE_MIN_LEN`, `SCRAPE_VISIT_FACTOR` (0 = sin límite) y `SCRAPE_MAX_VISITS` para acotar paradas de seguridad.
- Los trabajos automáticos (`scrape_auto_job`) leen esos mismos parámetros y permiten overrides puntuales con `job.data = {"max_pages": "full", "visit_factor": 8, ...}`.
- La deduplicación por URL en `data/scrape_seen.json` se mantiene para evitar reinsertar noticias ya procesadas.
