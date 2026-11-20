-- El catálogo 'lakehouse' se creó desde la interfaz (Catálogo > Agregar un catálogo)
-- con almacenamiento por defecto. CREATE CATALOG por SQL pidió una ubicación gestionada.

CREATE SCHEMA IF NOT EXISTS lakehouse.bronze COMMENT 'Datos crudos, tal como llegan del origen';
CREATE SCHEMA IF NOT EXISTS lakehouse.silver COMMENT 'Datos limpios y validados';
CREATE SCHEMA IF NOT EXISTS lakehouse.gold   COMMENT 'Agregados listos para consumo';
CREATE VOLUME IF NOT EXISTS lakehouse.bronze.landing COMMENT 'Zona de aterrizaje de archivos crudos';

-- Grupo 'analistas': creado en Configuración > Identidad y acceso > Grupos
GRANT USE CATALOG ON CATALOG lakehouse TO `analistas`;
GRANT USE SCHEMA, SELECT ON SCHEMA lakehouse.gold TO `analistas`;