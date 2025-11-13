-- Vista simple de ventas diarias
CREATE VIEW IF NOT EXISTS ventas_diarias AS
SELECT fecha_venta, SUM(unidades*precio_unitario) AS importe_total, COUNT(*) AS lineas
FROM clean_ventas
GROUP BY fecha_venta;

    -- Vista de ventas por categoria de producto
    CREATE VIEW IF NOT EXISTS ventas_por_categoria AS
    SELECT p.categoria, SUM(v.unidades * v.precio_unitario) AS importe_total, COUNT(*) AS lineas
    FROM clean_ventas v
    JOIN clean_productos p ON v.id_producto = p.id_producto
    GROUP BY p.categoria;

    -- Vista de productos más vendidos
    CREATE VIEW IF NOT EXISTS productos_mas_vendidos AS
    SELECT p.id_producto, p.nombre_producto, SUM(v.unidades) AS total_vendido
    FROM clean_productos p
    JOIN clean_ventas v ON p.id_producto = v.id_producto
    GROUP BY p.id_producto, p.nombre_producto
    ORDER BY total_vendido DESC
    LIMIT 10;

    -- Vista de clientes con mayor gasto
    CREATE VIEW IF NOT EXISTS clientes_mayor_gasto AS
    SELECT c.id_cliente, c.nombre, SUM(v.unidades * v.precio_unitario) AS gasto_total
    FROM clean_clientes c
    JOIN clean_ventas v ON c.id_cliente = v.id_cliente
    GROUP BY c.id_cliente, c.nombre
    ORDER BY gasto_total DESC
    LIMIT 10;