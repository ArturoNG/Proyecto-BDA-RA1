-- UPSERT para clean_productos
INSERT INTO clean_productos AS c (id_producto, nombre_producto, categoria, precio_unitario, unidades, fecha_entrada, _ingest_ts)
VALUES (:id_producto, :nombre_producto, :categoria, :precio_unitario, :unidades, :fecha_entrada, :_ingest_ts)
ON CONFLICT(id_producto) DO UPDATE SET
  nombre_producto = excluded.nombre_producto,
  categoria = excluded.categoria,
  precio_unitario = excluded.precio_unitario,
  unidades = excluded.unidades,
  fecha_entrada = excluded.fecha_entrada,
  _ingest_ts = excluded._ingest_ts
WHERE excluded._ingest_ts > c._ingest_ts;
