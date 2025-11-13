-- VENTAS
CREATE TABLE IF NOT EXISTS raw_ventas(
  fecha_venta TEXT, id_cliente TEXT, id_producto TEXT, unidades REAL, precio_unitario REAL,
  _ingest_ts TEXT, _source_file TEXT, _batch_id TEXT
);

CREATE TABLE IF NOT EXISTS clean_ventas(
  fecha_venta TEXT, id_cliente TEXT, id_producto TEXT, unidades REAL, precio_unitario REAL,
  _ingest_ts TEXT,
  PRIMARY KEY (fecha_venta, id_cliente, id_producto)
);

CREATE TABLE IF NOT EXISTS quarantine_ventas(
  _reason TEXT, _row TEXT, _ingest_ts TEXT, _source_file TEXT, _batch_id TEXT
);

-- CLIENTES
CREATE TABLE IF NOT EXISTS raw_clientes(
  fecha TEXT, nombre TEXT, apellido TEXT, id_cliente TEXT,
  _ingest_ts TEXT, _source_file TEXT, _batch_id TEXT
);

CREATE TABLE IF NOT EXISTS clean_clientes(
  fecha TEXT, nombre TEXT, apellido TEXT, id_cliente TEXT,
  _ingest_ts TEXT,
  PRIMARY KEY (id_cliente)
);

CREATE TABLE IF NOT EXISTS quarantine_clientes(
  _reason TEXT, _row TEXT, _ingest_ts TEXT, _source_file TEXT, _batch_id TEXT
);

-- PRODUCTOS
CREATE TABLE IF NOT EXISTS raw_productos(
  fecha_entrada TEXT, nombre_producto TEXT, id_producto TEXT, precio_unitario REAL, unidades REAL, categoria TEXT,
  _ingest_ts TEXT, _source_file TEXT, _batch_id TEXT
);

CREATE TABLE IF NOT EXISTS clean_productos(
  fecha_entrada TEXT, nombre_producto TEXT, id_producto TEXT, precio_unitario REAL, unidades REAL, categoria TEXT,
  _ingest_ts TEXT,
  PRIMARY KEY (id_producto)
);

CREATE TABLE IF NOT EXISTS quarantine_productos(
  _reason TEXT, _row TEXT, _ingest_ts TEXT, _source_file TEXT, _batch_id TEXT
);
