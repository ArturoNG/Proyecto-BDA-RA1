
from pathlib import Path
from datetime import datetime, timezone
import pandas as pd
import sqlite3

# Rutas
ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data" / "drops"
OUT = ROOT / "output"
OUT.mkdir(parents=True, exist_ok=True)
(OUT / "parquet").mkdir(parents=True, exist_ok=True)
(OUT / "quality").mkdir(parents=True, exist_ok=True)

# Conexión SQLite
DB = OUT / "ut1.db"
if DB.exists():
    DB.unlink()  # elimina el archivo de base de datos previo
con = sqlite3.connect(DB)

# Crear tablas en 00_schema.sql
con.executescript((ROOT / "sql" / "00_schema.sql").read_text(encoding="utf-8"))
print("Tablas creadas:", con.execute("SELECT name FROM sqlite_master WHERE type='table';").fetchall())

# Columnas en un diccionario para cada .csv
EXPECTED_COLUMNS = {
    "ventas":   ["fecha_venta", "id_cliente", "id_producto", "unidades", "precio_unitario"],
    "clientes": ["fecha", "nombre", "apellido", "id_cliente"],
    "productos":["fecha_entrada", "nombre_producto", "id_producto", "unidades", "precio_unitario", "categoria"]
}

# 1) INGESTA / BRONZE
def ingest_csv(path: Path, expected_cols: list, raw_table: str):
    """Lee un CSV, añade metadatos, normaliza claves y lo guarda en tabla raw_*"""
    if not path.exists():
        print(f"⚠️ No se encontró {path.name}")
        return pd.DataFrame(columns=expected_cols + ["_source_file","_ingest_ts","_batch_id"])
    
    df = pd.read_csv(path, dtype=str)
    # Normalizar nombres de columnas
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
    # Asegurar columnas esperadas
    for c in expected_cols:
        if c not in df.columns:
            df[c] = None

    # 🔑 Normalizar claves comunes
    if "id_cliente" in df.columns:
        df["id_cliente"] = df["id_cliente"].astype(str).str.strip().str.upper()
    if "id_producto" in df.columns:
        df["id_producto"] = df["id_producto"].astype(str).str.strip().str.upper()

    # Metadatos
    df["_source_file"] = path.name
    df["_ingest_ts"] = datetime.now(timezone.utc).isoformat()
    df["_batch_id"] = "demo"
    # Persistencia en SQLite (raw)
    df.to_sql(raw_table, con, if_exists="append", index=False)
    print(f"Ingested {len(df)} rows into {raw_table}")
    return df

# 2) Limpieza y persistencia / PLATA
def limpiar_y_persistir(df_raw, entity, con, upsert_sql_file, valid_fn, cols_clean):
    if df_raw.empty:
        print(f"No hay datos para {entity}")
        return

    df = df_raw.copy()

    # Validación
    valid_mask = valid_fn(df)
    quarantine = df.loc[~valid_mask].copy()
    clean = df.loc[valid_mask].copy()

    # Guardar quarantine
    quarantine.to_csv(OUT / "quality" / f"{entity}_invalidos.csv", index=False)
    print(f"Identificados {len(quarantine)} registros inválidos en {entity}, guardados en quarantine.")
    # Guardar quarantine en SQLite
    quarantine_table = f"quarantine_{entity}"
    quarantine.to_sql(quarantine_table, con, if_exists="replace", index=False)
    print(f"Identificados {len(quarantine)} registros inválidos en {entity}, guardados en CSV y SQLite ({quarantine_table})")
    
    # Deduplicar
    if not clean.empty:
        clean = (clean.sort_values("_ingest_ts")
                    .drop_duplicates(subset=cols_clean, keep="last"))

        # Persistencia en Parquet
        parquet_file = OUT / "parquet" / f"clean_{entity}.parquet"
        clean.to_parquet(parquet_file, index=False)

        # Persistencia en SQLite vía UPSERT
        upsert_sql = Path(upsert_sql_file).read_text(encoding="utf-8")
        for _, r in clean.iterrows():
            con.execute(upsert_sql, {k: r[k] for k in cols_clean + ["_ingest_ts"]})
        con.commit()

        print(f"Persistidos {len(clean)} registros limpios en {entity} (SQLite + Parquet)")

# 3) VALIDADORES
import pandas as pd

def validar_ventas(df):
    unidades = pd.to_numeric(df["unidades"], errors="coerce")
    precios = pd.to_numeric(df["precio_unitario"], errors="coerce")
    return (
        df["fecha_venta"].notna()
        & df["id_cliente"].notna() & (df["id_cliente"] != "")
        & df["id_producto"].notna() & (df["id_producto"] != "")
        & unidades.notna() & (unidades >= 0 )
        & precios.notna() & (precios >= 0)
    )

def validar_clientes(df):
    return (
        df["fecha"].notna()
        & df["nombre"].notna() & (df["nombre"] != "")
        & df["apellido"].notna() & (df["apellido"] != "")
        & df["id_cliente"].notna() & (df["id_cliente"] != "")
    )

def validar_productos(df):
    unidades = pd.to_numeric(df["unidades"], errors="coerce")
    precios = pd.to_numeric(df["precio_unitario"], errors="coerce")
    return (
        df["fecha_entrada"].notna()
        & df["nombre_producto"].notna() & (df["nombre_producto"] != "")
        & df["id_producto"].notna() & (df["id_producto"] != "")
        & unidades.notna() & (unidades >= 0)
        & precios.notna() & (precios >= 0)
        & df["categoria"].notna() & (df["categoria"] != "")
    )


if __name__ == "__main__":

    # 1) Ingesta
    raw_ventas = ingest_csv(DATA / "ventas.csv", EXPECTED_COLUMNS["ventas"], "raw_ventas")
    raw_clientes = ingest_csv(DATA / "clientes.csv", EXPECTED_COLUMNS["clientes"], "raw_clientes")
    raw_productos = ingest_csv(DATA / "productos.csv", EXPECTED_COLUMNS["productos"], "raw_productos")

    # 2) Limpieza + Persistencia (PLATA)
    limpiar_y_persistir(
        raw_ventas, "ventas", con,
        ROOT / "sql" / "10_upsert_ventas.sql",
        validar_ventas,
        ["fecha_venta", "id_cliente", "id_producto", "unidades", "precio_unitario"]
    )

    limpiar_y_persistir(
        raw_clientes, "clientes", con,
        ROOT / "sql" / "11_upsert_clientes.sql",
        validar_clientes,
        ["id_cliente", "nombre", "apellido", "fecha"]
    )

    limpiar_y_persistir(
        raw_productos, "productos", con,
        ROOT / "sql" / "12_upsert_productos.sql",
        validar_productos,
        ["id_producto", "nombre_producto", "categoria", "precio_unitario", "unidades", "fecha_entrada"]
    )

# Vistas
con.executescript((ROOT / "sql" / "20_views.sql").read_text(encoding="utf-8"))
con.close()

# 5) Reporte desde parquet. / GOLD
ventas_file = OUT / "parquet" / "clean_ventas.parquet"
clientes_file = OUT / "parquet" / "clean_clientes.parquet"
productos_file = OUT / "parquet" / "clean_productos.parquet"

if ventas_file.exists():
    ventas = pd.read_parquet(ventas_file)
else:
    ventas = pd.DataFrame(columns=["fecha_venta","id_cliente","id_producto","unidades","precio_unitario","_ingest_ts"])

if clientes_file.exists():
    clientes = pd.read_parquet(clientes_file)
else:
    clientes = pd.DataFrame(columns=["id_cliente","nombre","apellido","fecha"])

if productos_file.exists():
    productos = pd.read_parquet(productos_file)
else:
    productos = pd.DataFrame(columns=["id_producto","nombre_producto","categoria","precio_unitario","unidades","fecha_entrada"])

if not ventas.empty:
    ventas["importe"] = ventas["unidades"].astype(float) * ventas["precio_unitario"].astype(float)

    # Left join para mostrar nombres de clientes y productos en ventas.
    ventas = ventas.merge(clientes[["id_cliente","nombre","apellido"]], on="id_cliente", how="left")
    ventas = ventas.merge(productos[["id_producto","nombre_producto","categoria"]], on="id_producto", how="left")

    ingresos = ventas["importe"].sum()
    trans = len(ventas)
    ticket = ingresos / trans if trans > 0 else 0.0

    top_prod = (ventas.groupby(["id_producto","nombre_producto","categoria"], as_index=False)
                    .agg(importe=("importe","sum"))
                    .sort_values("importe", ascending=False).head(5))
    total_imp = top_prod["importe"].sum() or 1.0
    top_prod["pct"] = (100 * top_prod["importe"] / total_imp).round(0).astype(int).astype(str) + "%"

    top_cli = (ventas.groupby("id_cliente", as_index=False)
                .agg(importe=("importe","sum"))
                .sort_values("importe", ascending=False)
                .head(5)
                .merge(clientes[["id_cliente","nombre","apellido"]], on="id_cliente", how="left"))

    by_day = (ventas.groupby("fecha_venta", as_index=False)
                    .agg(importe_total=("importe","sum"),
                        transacciones=("importe","count")).head(10))
    
    mayor_venta = ventas.loc[ventas["importe"].idxmax()]
    mayor_venta_info = (
        f"- Fecha: {mayor_venta['fecha_venta']}\n"
        f"- Cliente: {mayor_venta['nombre']} {mayor_venta['apellido']} (ID: {mayor_venta['id_cliente']})\n"
        f"- Producto: {mayor_venta['nombre_producto']} (ID: {mayor_venta['id_producto']})\n"
        f"- Unidades: {mayor_venta['unidades']}\n"
        f"- Importe: {mayor_venta['importe']:.2f} €") if not mayor_venta.empty else "—" 
    
    periodo_ini = str(ventas["fecha_venta"].min())
    periodo_fin = str(ventas["fecha_venta"].max())
    producto_lider = top_prod.iloc[0]["nombre_producto"] if not top_prod.empty else "—"
else:
    ingresos = 0.0; ticket = 0.0; trans = 0
    top_prod = pd.DataFrame(columns=["id_producto","nombre_producto","categoria","importe","pct"])
    top_cli = pd.DataFrame(columns=["id_cliente","nombre","apellido","importe"])
    by_day = pd.DataFrame(columns=["fecha_venta","importe_total","transacciones"])
    periodo_ini = "—"; periodo_fin = "—"; producto_lider = "—"

report = (
    "# Reporte UT1 · Ventas\n"
    f"**Periodo:** {periodo_ini} a {periodo_fin} · **Fuente:** clean_ventas (Parquet) · **Generado:** {datetime.now(timezone.utc).isoformat()}\n\n"
    "## 1. Titular\n"
    f"Ingresos totales {ingresos:.2f} €; producto líder: {producto_lider}.\n\n"
    "## 2. KPIs\n"
    f"- **Ingresos netos:** {ingresos:.2f} €\n"
    f"- **Ticket medio:** {ticket:.2f} €\n"
    f"- **Transacciones:** {trans}\n\n"
    "## 3. Top 5 productos\n"
    f"{(top_prod.to_markdown(index=False) if not top_prod.empty else '_(sin datos)_')}\n\n"
    "## 4. Top 5 clientes\n"
    f"{(top_cli.to_markdown(index=False) if not top_cli.empty else '_(sin datos)_')}\n\n"
    "## 5. Resumen por últimos 10 días\n"
    f"{(by_day.to_markdown(index=False) if not by_day.empty else '_(sin datos)_')}\n\n"  # <-- agregado aquí
    "## 6. Mayor venta\n"
    f"{mayor_venta_info}\n\n"
    "## 7. Persistencia\n"
    f"- Parquet: {ventas_file}, {clientes_file}, {productos_file}\n"
    f"- SQLite : {DB} (tablas: raw_*, clean_*; vistas: ventas_diarias)\n\n"
    "## 8. Conclusiones\n"
    "- Reponer producto líder según demanda.\n"
    "- Identificar clientes clave para fidelización.\n"
    "- Revisar filas en cuarentena (rangos/tipos).\n"
    "- Valorar particionado por fecha para crecer.\n"
)

(OUT / "reporte.md").write_text(report, encoding="utf-8")
print("✅ Reporte generado:", OUT / "reporte.md")
