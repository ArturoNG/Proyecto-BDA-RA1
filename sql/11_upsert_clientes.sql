-- UPSERT para clean_clientes
INSERT INTO clean_clientes AS c (id_cliente, nombre, apellido, fecha, _ingest_ts)
VALUES (:id_cliente, :nombre, :apellido, :fecha, :_ingest_ts)
ON CONFLICT(id_cliente) DO UPDATE SET
  nombre = excluded.nombre,
  apellido = excluded.apellido,
  fecha = excluded.fecha,
  _ingest_ts = excluded._ingest_ts
WHERE excluded._ingest_ts > c._ingest_ts;
