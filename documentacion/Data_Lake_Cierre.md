# Cierre del Data Lake — Arquitecturas de Datos 2026-2

## Requisitos originales y estado

Basado en «Actividad Arquitecturas de Datos.pdf», páginas 6–13.

| Requisito | Estado de trabajo | Acción pendiente |
|---|---|---|
| RSS tres secciones | Implementado | Conservar XML y manifiestos |
| S3 Bronze privado | Ejecutado previamente | Revisar evidencias y cargos |
| Silver noticias y quarantine | Implementado local | Conciliar registros inválidos y cambios |
| Silver enriquecido con Comprehend | No confirmado | Obtener permiso, ejecutar Comprehend real, registrar hash/idioma/fecha/score/cache |
| Silver `news_entities`, `news_key_phrases` | No confirmado | Publicar Parquet real con una fila por entidad/frase distinta por noticia |
| Gold noticias/día/sección | Implementado local y probado antes en Athena | Comprobar snapshot correcto |
| Gold entidades, frases y evolución | Incluido en el paquete local | Requiere resultados Comprehend verificados |
| Glue y Athena Silver/Gold completos | Parcial | Crear y registrar tablas y particiones finales |
| Airflow local | Probado | No equivale al Airflow en EC2 exigido |
| Airflow en EC2 | No realizado | Requiere presupuesto/autorización del profesor |
| 9 escenarios 4.5 | Parcial | Guardar evidencias de defectuosos, nuevas versiones, reprocesamiento y nuevas particiones |
| Entrega documental y demostración | Parcial | Completar README, arquitectura, costos, limpieza y capturas |

## Cómo ejecutar el verificador local

Desde Ubuntu WSL:

```bash
conda activate aws-datalake
cd ~/Arquitecturas-Datos-AWS/data-lake
python src/cerrar_datalake_local.py --project .
```

El script encuentra el último snapshot local exitoso de `airflow_end_to_end` o usa `local/silver/news_current.parquet`. Produce los cuatro conjuntos analíticos y un `reporte_cierre.json` bajo `local/cierre_datalake/snapshot_id=...`.

**Si no tiene resultados verificados de Comprehend**, las métricas NLP quedan vacías y el reporte lo indica: `origen_nlp=SIN_COMPREHEND`, `cumple_comprehend_real=false`. Nunca confundir un cero pendiente con ausencia real de entidades.

Para procesar resultados propios **verificados** de Comprehend, preparar tres archivos Parquet (no CSV) con columnas:

- `--entities`: `news_id`, `text_hash`, `entity_type`, `entity_text`, `score`. Tipo `PERSON`, `ORGANIZATION` o `LOCATION`.
- `--phrases`: `news_id`, `text_hash`, `phrase_text`, `score`.
- `--status`: `news_id`, `text_hash`, `enrichment_status`, `language`, `analyzed_at`. Valores de status: `success`, `pending`, `skipped`, `error`.

Los resultados sin coincidencia del `text_hash`, sin noticia asociada o con puntajes inválidos se rechazan. Los datos deben corresponder a las noticias del snapshot vigente. Las noticias sin entidades pueden legítimamente tener estado `success` y cero filas de entidades.

Ejemplo de ejecución con datos reales ya procesados (solo si existen y tienen licencia de uso):

```bash
python src/cerrar_datalake_local.py --project . \
  --entities RUTA/news_entities.parquet \
  --phrases RUTA/news_key_phrases.parquet \
  --status RUTA/news_enrichment_status.parquet \
  --provenance aws_comprehend
```

El script **no llama Comprehend**; recibe sus resultados. Los resultados sintéticos de prueba deben marcarse siempre como `--provenance demo`, sin afirmar que son resultados del proveedor.

## Diccionario de datos: agregado final

- `news_by_day`: granularidad fecha UTC de publicación × medio × sección; `total_noticias` cuenta `news_id` distintos en esa combinación. La misma noticia puede contar en dos secciones si aparece en dos RSS.
- `entities_ranking`: tipo (`PERSON`, `ORGANIZATION`, `LOCATION`) × texto de entidad; cuenta noticias distintas globalmente.
- `entity_trend_by_day`: fecha UTC × tipo × texto de entidad; cuenta noticias distintas, sin doble contabilizar menciones repetidas.
- `phrases_by_section`: medio × sección × texto de frase; cuenta noticias distintas por sección.

Las tablas de entidades y frases **solo son interpretables después de confirmar que Comprehend procesó los textos del snapshot vigente**.

## Arquitectura

```text
RSS El Tiempo (3 feeds)
        ↓
Bronze XML + manifiestos (S3; espejo local)
        ↓
Silver news + cuarentena + eventos (Parquet)
        ↓
[Enriquecimiento Comprehend con hash y caché: PENDIENTE]
        ↓
news_entities, news_key_phrases, news_enrichment_status
        ↓
Gold: noticias/día, ranking entidades, evolución, frases/sección
        ↓
Glue Catalog → Athena SQL
        ↑
Airflow orquesta (actual: local WSL; requerido: EC2)
```

## Pruebas y límites

- Control de unicidad de `news_id`, fechas, enlaces y campos obligatorios.
- Trazabilidad a Bronze: asegurar `run_id` y `bronze_s3_key` (o ruta local equivalente) en las noticias.
- Repetir un lote no debe sumar noticias ni sobreescribir un snapshot válido con otros datos.
- Las entidades/frases no deben tener `news_id` huérfanos, y su `text_hash` debe corresponder al de la noticia vigente.
- Comprehend debe registrar texto, hash, idioma, tiempo de análisis, puntajes, límite de texto y estado; reusar resultados si el texto no cambió.
- Si faltan fechas válidas, registrar rechazo trazable; no asignar fechas inventadas.
- AWS Glue: `MSCK REPAIR TABLE` agrega particiones y no elimina las antiguas. Evitar consultar varios snapshots simultáneamente como una sola tabla.
- Este cierre solo **valida y produce archivos locales**, NO realiza la publicación S3/Glue/Athena ni sustituye EC2.

## Evidencias restantes

- `48_cierre_gold_local.png`: reporte_cierre.json del último snapshot y sus conteos.
- `49_pruebas_cierre.png`: tests de calidad completados.
- `50_consultas_athena_entidades.png`: solo si se publican realmente las tablas con entidades y se ejecutan consultas.
- `51_glue_gold_completo.png`: solo si existen las tablas correspondientes en AWS Glue.
- `52_comprehend_real.png`: solo con resultados reales y autorizados, no con demo sintética.

## Costos y seguridad

No ejecutar nuevas consultas Athena ni API Comprehend ni iniciar EC2 sin presupuesto/autorización. El uso local evita **nuevas llamadas** por este script, pero S3 y otros recursos ya existentes pueden mantener cargos. Mantener bucket privado; no guardar claves AWS en repositorio. Al finalizar, solicitar autorización del titular de la cuenta para eliminar recursos facturables, después de respaldar las evidencias.

## Lista de archivos para GitHub

- `data-lake/src/`, `data-lake/dags/` y `infrastructure/airflow-local/dags/`;
- `data-lake/sql/`, diccionario de datos, README, diagrama y `.env.example`;
- evidencias con datos sensibles ocultos;
- **no** subir `.env`, credenciales, cachés privados, registros de acceso o datos protegidos sin permiso.
