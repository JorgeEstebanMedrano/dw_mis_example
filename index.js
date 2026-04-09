require('dotenv').config(); // ¡LÍNEA 1!
const express = require('express');
const { Pool } = require('pg');
const path = require('path');

const app = express();
const port = process.env.PORT || 3000;

app.set('view engine', 'ejs');
app.set('views', path.join(__dirname, 'views'));

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false }
});

async function ejecutarETL() {
  let cliente;
  try {
    console.log("Iniciando extracción y transformación ETL...");
    cliente = await pool.connect();

    // 0. LIMPIEZA PREVIA: Borramos los esquemas anteriores para evitar conflictos
    await cliente.query(`
      DROP SCHEMA IF EXISTS staging CASCADE;
      DROP SCHEMA IF EXISTS dw CASCADE;
    `);

    // FASE 1: STAGING AREA
    await cliente.query(`
      CREATE SCHEMA staging;
      
      CREATE TABLE staging.alquileres_crudos AS
      SELECT 
        r.rental_id, DATE(r.rental_date) as fecha,
        c.customer_id, c.first_name || ' ' || c.last_name AS cliente,
        f.film_id, f.title AS pelicula, f.rating,
        st.store_id, ci.city AS ciudad_tienda,
        s.staff_id, s.first_name || ' ' || s.last_name AS empleado,
        COALESCE(p.amount, 0) as monto
      FROM public.rental r
      JOIN public.customer c ON r.customer_id = c.customer_id
      JOIN public.inventory i ON r.inventory_id = i.inventory_id
      JOIN public.film f ON i.film_id = f.film_id
      JOIN public.store st ON i.store_id = st.store_id
      JOIN public.address a ON st.address_id = a.address_id
      JOIN public.city ci ON a.city_id = ci.city_id
      JOIN public.staff s ON r.staff_id = s.staff_id
      LEFT JOIN public.payment p ON r.rental_id = p.rental_id;
    `);

    // FASE 2: MODELIZACIÓN FÍSICA
    await cliente.query(`
      CREATE SCHEMA dw;

      CREATE TABLE dw.d_tiempo (id_tiempo SERIAL PRIMARY KEY, fecha DATE UNIQUE);
      CREATE TABLE dw.d_cliente (id_cliente INT PRIMARY KEY, nombre VARCHAR(100));
      CREATE TABLE dw.d_pelicula (id_pelicula INT PRIMARY KEY, titulo VARCHAR(255), clasificacion VARCHAR(10));
      CREATE TABLE dw.d_tienda (id_tienda INT PRIMARY KEY, ciudad VARCHAR(100));
      CREATE TABLE dw.d_empleado (id_empleado INT PRIMARY KEY, nombre VARCHAR(100));
      
      CREATE TABLE dw.h_ingresos (
        id_tiempo INT, id_cliente INT, id_pelicula INT, id_tienda INT, id_empleado INT,
        cantidad_alquileres INT, monto_total DECIMAL(10,2),
        PRIMARY KEY (id_tiempo, id_cliente, id_pelicula, id_tienda, id_empleado)
      );
    `);

    // FASE 3: CARGA AL DATA WAREHOUSE
    await cliente.query(`
      INSERT INTO dw.d_tiempo (fecha) SELECT DISTINCT fecha FROM staging.alquileres_crudos ON CONFLICT DO NOTHING;
      INSERT INTO dw.d_cliente (id_cliente, nombre) SELECT DISTINCT customer_id, cliente FROM staging.alquileres_crudos ON CONFLICT DO NOTHING;
      INSERT INTO dw.d_pelicula (id_pelicula, titulo, clasificacion) SELECT DISTINCT film_id, pelicula, rating::text FROM staging.alquileres_crudos ON CONFLICT DO NOTHING;
      INSERT INTO dw.d_tienda (id_tienda, ciudad) SELECT DISTINCT store_id, ciudad_tienda FROM staging.alquileres_crudos ON CONFLICT DO NOTHING;
      INSERT INTO dw.d_empleado (id_empleado, nombre) SELECT DISTINCT staff_id, empleado FROM staging.alquileres_crudos ON CONFLICT DO NOTHING;

      INSERT INTO dw.h_ingresos (id_tiempo, id_cliente, id_pelicula, id_tienda, id_empleado, cantidad_alquileres, monto_total)
      SELECT 
        t.id_tiempo, s.customer_id, s.film_id, s.store_id, s.staff_id,
        COUNT(s.rental_id), SUM(s.monto)
      FROM staging.alquileres_crudos s
      JOIN dw.d_tiempo t ON s.fecha = t.fecha
      GROUP BY t.id_tiempo, s.customer_id, s.film_id, s.store_id, s.staff_id
      ON CONFLICT DO NOTHING;
    `);
    console.log("Proceso de Data Warehouse finalizado exitosamente.");
  } catch (err) {
    console.error("Error en ETL:", err.message);
  } finally {
    if (cliente) cliente.release();
  }
}

// ================= RUTAS DE LA EXPOSICIÓN =================

// PASO 1: Extracción de Fuentes (Staging)
app.get('/', async (req, res) => {
  const result = await pool.query('SELECT * FROM staging.alquileres_crudos LIMIT 12');
  res.render('fase1_staging', { datos: result.rows });
});

// PASO 2: Modelización (Lectura dinámica del Information Schema)
app.get('/esquema', async (req, res) => {
  try {
    // Contamos dinámicamente cuántas tablas tiene cada esquema
    const conteo = await pool.query(`
      SELECT table_schema, COUNT(*) as total_tablas
      FROM information_schema.tables 
      WHERE table_schema IN ('public', 'staging', 'dw')
      GROUP BY table_schema;
    `);
    
    const stats = { public: 0, staging: 0, dw: 0 };
    conteo.rows.forEach(r => stats[r.table_schema] = parseInt(r.total_tablas));

    res.render('fase2_esquema', { stats });
  } catch (err) {
    res.status(500).send("Error cargando los modelos: " + err.message);
  }
});

// PASO 3: Desarrollo DW (Vista del Cubo Multidimensional)
app.get('/etl', async (req, res) => {
  // Extraemos una pequeña muestra de las 5 dimensiones
  const dims = {
    tiempo: await pool.query('SELECT * FROM dw.d_tiempo LIMIT 3'),
    cliente: await pool.query('SELECT * FROM dw.d_cliente LIMIT 3'),
    pelicula: await pool.query('SELECT * FROM dw.d_pelicula LIMIT 3'),
    tienda: await pool.query('SELECT * FROM dw.d_tienda LIMIT 3'),
    empleado: await pool.query('SELECT * FROM dw.d_empleado LIMIT 3')
  };

  // Consultamos el "Cubo OLAP": La tabla de hechos cruzada con TODAS sus dimensiones
  const cubo = await pool.query(`
    SELECT 
      t.fecha, c.nombre as cliente, p.titulo as pelicula, 
      st.ciudad as tienda, e.nombre as empleado, h.monto_total
    FROM dw.h_ingresos h
    JOIN dw.d_tiempo t ON h.id_tiempo = t.id_tiempo
    JOIN dw.d_cliente c ON h.id_cliente = c.id_cliente
    JOIN dw.d_pelicula p ON h.id_pelicula = p.id_pelicula
    JOIN dw.d_tienda st ON h.id_tienda = st.id_tienda
    JOIN dw.d_empleado e ON h.id_empleado = e.id_empleado
    ORDER BY h.monto_total DESC
    LIMIT 10
  `);

  res.render('fase3_etl', { dims, cubo: cubo.rows });
});

// PASO 4: Explotación BI (Dashboard de 6 Gráficos)
app.get('/bi', async (req, res) => {
  try {
    // 1. Dimensión Tiempo
    const qTiempo = await pool.query(`SELECT TO_CHAR(t.fecha, 'YYYY-MM-DD') as fecha, SUM(h.monto_total) as total FROM dw.h_ingresos h JOIN dw.d_tiempo t ON h.id_tiempo = t.id_tiempo GROUP BY t.fecha ORDER BY t.fecha LIMIT 15`);
    // 2. Dimensión Película (Top Títulos)
    const qPelicula = await pool.query(`SELECT p.titulo, SUM(h.monto_total) as total FROM dw.h_ingresos h JOIN dw.d_pelicula p ON h.id_pelicula = p.id_pelicula GROUP BY p.titulo ORDER BY total DESC LIMIT 5`);
    // 3. Dimensión Película (Clasificación/Rating)
    const qClasificacion = await pool.query(`SELECT p.clasificacion, SUM(h.monto_total) as total FROM dw.h_ingresos h JOIN dw.d_pelicula p ON h.id_pelicula = p.id_pelicula GROUP BY p.clasificacion`);
    // 4. Dimensión Empleado
    const qEmpleado = await pool.query(`SELECT e.nombre, SUM(h.monto_total) as total FROM dw.h_ingresos h JOIN dw.d_empleado e ON h.id_empleado = e.id_empleado GROUP BY e.nombre`);
    // 5. Dimensión Tienda
    const qTienda = await pool.query(`SELECT t.ciudad, SUM(h.monto_total) as total FROM dw.h_ingresos h JOIN dw.d_tienda t ON h.id_tienda = t.id_tienda GROUP BY t.ciudad`);
    // 6. Dimensión Cliente (Top por cantidad de alquileres, no por monto)
    const qCliente = await pool.query(`SELECT c.nombre, SUM(h.cantidad_alquileres) as total_alquileres FROM dw.h_ingresos h JOIN dw.d_cliente c ON h.id_cliente = c.id_cliente GROUP BY c.nombre ORDER BY total_alquileres DESC LIMIT 5`);

    res.render('fase4_bi', {
      tiempo: qTiempo.rows, 
      pelicula: qPelicula.rows, 
      clasificacion: qClasificacion.rows,
      empleado: qEmpleado.rows, 
      tienda: qTienda.rows,
      cliente: qCliente.rows
    });
  } catch (err) {
    res.status(500).send("Error cargando el BI: " + err.message);
  }
});

app.listen(port, () => {
  console.log(`Servidor corriendo en el puerto ${port}`);
  ejecutarETL();
}); 