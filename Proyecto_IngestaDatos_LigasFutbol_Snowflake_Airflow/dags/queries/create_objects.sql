-- Crear el WH
create or replace warehouse wh_lf warehouse_size = XSMALL initially_suspended = true;
-- Crear la DB
create database snowflake_ligasfutbol;
-- Crear el objeto de alamcenamiento(stage)
Use snowflake_ligasfutbol;
create or replace stage stage_lf;
------------------------------------------------------------
-- Crear la TABLA que contendrá los datos de las LIGAS
CREATE OR REPLACE TABLE ligas_futbol(
    id VARCHAR(30) NOT NULL,
    equipo VARCHAR(30) NOT NULL,
    jugados INTEGER NOT NULL,
    ganados INTEGER NOT NULL,
    empatados INTEGER NOT NULL,
    perdidos INTEGER NOT NULL,
    GF INTEGER NOT NULL,
    GC INTEGER NOT NULL,
    diff INTEGER NOT NULL,
    puntos INTEGER NOT NULL,
    liga VARCHAR(30) NOT NULL,
    created_at VARCHAR(30) NOT NULL
);
------------------------------------------------------------
-- Listar el STAGE
list @stage_lf;

-- Ver la estructura de la tabla
SELECT * FROM "SNOWFLAKE_LIGASFUTBOL"."PUBLIC"."LIGAS_FUTBOL"