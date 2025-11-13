-- UPSERT para clean_ventas
INSERT INTO clean_ventas AS c (fecha_venta, id_cliente, id_producto, unidades, precio_unitario, _ingest_ts)
VALUES (:fecha_venta, :id_cliente, :id_producto, :unidades, :precio_unitario, :_ingest_ts)
ON CONFLICT(fecha_venta, id_cliente, id_producto) DO UPDATE SET
  unidades = excluded.unidades,
  precio_unitario = excluded.precio_unitario,
  _ingest_ts = excluded._ingest_ts
WHERE excluded._ingest_ts > c._ingest_ts;
