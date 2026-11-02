# Resultados del Data Warehouse en AWS

## 3.1 Migracion
- RDS MySQL: 4 tablas y 446 registros.
- DocumentDB: 508 eventos y 158 reseñas.
- Total DocumentDB: 666 documentos.

## 3.2 S3
- Almacenamiento privado por fuente y lote.
- Archivos de datos y manifiestos de carga.

## 3.3 Redshift y dbt
- Esquemas: raw, intermediate, quarantine y marts.
- 33 modelos, 2 snapshots y 92 pruebas.
- Ejecucion exitosa: PASS=129, ERROR=0.

## 3.4 Airflow
- DAG: stefany_data_warehouse.
- Verificacion S3, conexion Redshift y dbt build.
- Ejecucion comprobada: Dag run in success state.

## 3.5 Validaciones
- Ventas: 253 registros sin claves duplicadas.
- Reseñas: 150 registros sin claves duplicadas.
- Navegacion: 500 registros sin claves duplicadas.
- Cuarentena: 32 registros rechazados.
- SCD2 clientes: 50 versiones y 50 clientes.
- SCD2 productos: 25 versiones y 25 productos.
- Snapshots configurados; sin versiones historicas adicionales observadas.
- Pruebas locales de recuperacion realizadas.

## Seguridad
- Roles IAM, Secrets Manager y almacenamiento S3 privado.
