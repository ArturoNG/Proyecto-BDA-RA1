# 📊 Proyecto UT1 — Limpieza y Análisis de Ventas

Este proyecto permite **leer datos de ventas, clientes y productos desde archivos CSV**, **limpiarlos automáticamente**, **guardarlos en una base de datos y en formato Parquet**, y finalmente **generar un informe resumen** en formato Markdown (`reporte.md`).

Su objetivo es facilitar la gestión de información de una pequeña empresa o tienda sin necesidad de usar Excel manualmente ni tener conocimientos de programación.

---

## 🧠 ¿Qué hace el programa?

Cuando ejecutas el proyecto, ocurren los siguientes pasos:

1. **Lee los datos** de los .csv de `data/drops`:  
   - `ventas.csv`  
   - `clientes.csv`  
   - `productos.csv`

2. **Comprueba y limpia los datos**:
   - Quita filas con datos incompletos o inválidos (por ejemplo, precios negativos o nombres vacíos).  
   - Guarda los registros válidos en formato **Parquet** (para análisis rápido).  
   - Guarda los registros inválidos en la carpeta `output/quality` para revisarlos más tarde.

3. **Guarda la información limpia en una base de datos SQLite**, que se crea automáticamente en `output/ut1.db`.

4. **Genera un informe resumen (`reporte.md`)** que incluye:
   - Total de ingresos y ticket medio.  
   - Los 5 productos más vendidos.  
   - Los 5 clientes con más compras.  
   - Evolución de ventas por día.  
   - Detalle de la venta más grande.  
   - Sugerencias y conclusiones.

De esta manera, tendrás todo el flujo completo de **ingesta → limpieza → análisis → reporte** sin necesidad de abrir Excel.

## 📁 Estructura de carpetas
project/
│
├── data/
│ └── drops/
│ ├── ventas.csv
│ ├── clientes.csv
│ └── productos.csv
│
├── sql/
│ ├── 00_schema.sql
│ ├── 10_upsert_ventas.sql
│ ├── 11_upsert_clientes.sql
│ └── 12_upsert_productos.sql
│
├── output/
│ ├── parquet/ # Archivos limpios en formato parquet
│ ├── quality/ # Registros con errores
│ ├── ut1.db # Base de datos SQLite generada
│ └── reporte.md # Informe generado automáticamente
│
└── ingest/
├── run.py # Script principal que ejecuta todo el proceso
└── get_data.py # (Opcional) genera CSVs de ejemplo


## Comandos
```bash
pip install -r requirements.txt
python ingest/get_data.py      # Genera el ventas.csv (opcional si lo has descargado directamente)
python ingest/run.py           # ejecuta todo: parquet + sqlite + reporte.md
```
