# Reporte UT1 · Ventas
**Periodo:** 2025-07-02 a 2025-07-31 · **Fuente:** clean_ventas (Parquet) · **Generado:** 2025-11-13T17:48:21.693450+00:00

## 1. Titular
Ingresos totales 1396209.00 €; producto líder: Parlante Bluetooth JBL.

## 2. KPIs
- **Ingresos netos:** 1396209.00 €
- **Ticket medio:** 10823.33 €
- **Transacciones:** 129

## 3. Top 5 productos
| id_producto   | nombre_producto        | categoria        |   importe | pct   |
|:--------------|:-----------------------|:-----------------|----------:|:------|
| P006          | Parlante Bluetooth JBL | electronica      |     83844 | 31%   |
| P047          | Componente 47          | oficina          |     48022 | 18%   |
| P115          | Artículo 115           | muebles          |     47960 | 18%   |
| P042          | Accesorio 42           | seguridad        |     46816 | 17%   |
| P036          | Producto Genérico 36   | electrodomestico |     44625 | 16%   |

## 4. Top 5 clientes
| id_cliente   |   importe | nombre   | apellido   |
|:-------------|----------:|:---------|:-----------|
| C018         |     78060 | Beatriz  | Castro     |
| C094         |     66930 | Carlos   | Castro     |
| C116         |     51050 | Sergio   | López      |
| C008         |     44349 | Laura    | Torres     |
| C038         |     43554 | Isabel   | Suárez     |

## 5. Resumen por últimos 10 días
| fecha_venta   |   importe_total |   transacciones |
|:--------------|----------------:|----------------:|
| 2025-07-02    |           31976 |               5 |
| 2025-07-03    |            1526 |               2 |
| 2025-07-04    |           77270 |               6 |
| 2025-07-05    |           60801 |               5 |
| 2025-07-06    |          104052 |               7 |
| 2025-07-07    |           95335 |               9 |
| 2025-07-08    |           56570 |               5 |
| 2025-07-09    |           58139 |               6 |
| 2025-07-10    |           57031 |               6 |
| 2025-07-11    |           72141 |               7 |

## 6. Mayor venta
- Fecha: 2025-07-06
- Cliente:  Lucía  Rodríguez (ID: C101)
- Producto: Equipo 85 (ID: P085)
- Unidades: 10
- Importe: 39530.00 €

## 7. Persistencia
- Parquet: C:\Users\Arturo\Documents\PROYECTOS IA Y BIG DATA\Ruben\RA1_GBD\Poryecto-BDA-RA1\output\parquet\clean_ventas.parquet, C:\Users\Arturo\Documents\PROYECTOS IA Y BIG DATA\Ruben\RA1_GBD\Poryecto-BDA-RA1\output\parquet\clean_clientes.parquet, C:\Users\Arturo\Documents\PROYECTOS IA Y BIG DATA\Ruben\RA1_GBD\Poryecto-BDA-RA1\output\parquet\clean_productos.parquet
- SQLite : C:\Users\Arturo\Documents\PROYECTOS IA Y BIG DATA\Ruben\RA1_GBD\Poryecto-BDA-RA1\output\ut1.db (tablas: raw_*, clean_*; vistas: ventas_diarias)

## 8. Conclusiones
- Reponer producto líder según demanda.
- Identificar clientes clave para fidelización.
- Revisar filas en cuarentena (rangos/tipos).
- Valorar particionado por fecha para crecer.
