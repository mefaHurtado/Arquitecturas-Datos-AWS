import argparse
import csv
import hashlib
import os
import re
from pathlib import Path

LOTE = "20261102T013140Z-c5e61618"
BASE = Path.home() / "dw-migracion-local/redshift-carga" / LOTE
SQL = Path(__file__).resolve().parents[1] / "sql/copy_raw_redshift.sql"

ENTIDADES = {
    "mysql_customers": 54,
    "mysql_products": 27,
    "mysql_orders": 106,
    "mysql_order_items": 259,
    "mongo_reviews": 158,
    "mongo_navigation_events": 508
}

parser = argparse.ArgumentParser()
parser.add_argument("--execute", action="store_true")
args = parser.parse_args()

contenido_sql = SQL.read_text(encoding="utf-8")

comandos = {}
for nombre, sentencia in re.findall(
    r"\bCOPY\s+raw\.(\w+)\b(.*?;)",
    contenido_sql,
    re.IGNORECASE | re.DOTALL
):
    if nombre in comandos:
        raise SystemExit(f"COPY duplicado: {nombre}")

    comandos[nombre] = f"COPY raw.{nombre}{sentencia}"

hashes = []
total = 0

print("=== PLAN DE CARGA REDSHIFT ===")

for entidad, esperado in ENTIDADES.items():
    archivo = BASE / "datos" / f"{entidad}.csv"
    manifiesto = BASE / "manifiestos" / f"{entidad}.manifest"

    if not archivo.is_file() or not manifiesto.is_file():
        raise SystemExit(f"Faltan archivos: {entidad}")

    if entidad not in comandos:
        raise SystemExit(f"Falta COPY: {entidad}")

    if f"{entidad}.manifest" not in comandos[entidad]:
        raise SystemExit(f"Manifiesto incorrecto: {entidad}")

    if "IAM_ROLE 'default'" not in comandos[entidad]:
        raise SystemExit(f"Revisar IAM_ROLE: {entidad}")

    with archivo.open(encoding="utf-8", newline="") as f:
        filas = list(csv.DictReader(f))

    if len(filas) != esperado:
        raise SystemExit(f"Cantidad incorrecta: {entidad}")

    if any(fila.get("_batch_id") != LOTE for fila in filas):
        raise SystemExit(f"Lote incorrecto: {entidad}")

    hashes.append(
        hashlib.sha256(archivo.read_bytes()).hexdigest()
    )
    total += len(filas)

    print(f"{entidad}: {len(filas)} registros | LISTO")

if total != 1112:
    raise SystemExit("Total inesperado")

huella = hashlib.sha256(
    "|".join(hashes).encode()
).hexdigest()

print("\n=== RESULTADO DE VALIDACION ===")
print("Archivos validados:", len(ENTIDADES))
print("Registros esperados:", total)
print("Lote:", LOTE)
print("SHA256 del conjunto:", huella)

if not args.execute:
    print("\nPLAN VALIDADO")
    print("CARGA REAL NO EJECUTADA")
    raise SystemExit(0)

variables = [
    "DW_REDSHIFT_HOST",
    "DW_REDSHIFT_USER",
    "DBT_ENV_SECRET_REDSHIFT_PASSWORD"
]

faltantes = [v for v in variables if not os.getenv(v)]

if faltantes:
    raise SystemExit("Faltan variables: " + ", ".join(faltantes))

confirmacion = input(
    "Esto ejecutara COPY en Redshift. Escribe CARGAR: "
)

if confirmacion != "CARGAR":
    raise SystemExit("Operacion cancelada")

import psycopg2

conexion = psycopg2.connect(
    host=os.environ["DW_REDSHIFT_HOST"],
    port=5439,
    user=os.environ["DW_REDSHIFT_USER"],
    password=os.environ["DBT_ENV_SECRET_REDSHIFT_PASSWORD"],
    dbname=os.getenv("DW_REDSHIFT_DATABASE", "ecommerce"),
    sslmode="require"
)

try:
    with conexion.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS raw.ingestion_batches (
                batch_id VARCHAR(100),
                content_hash VARCHAR(64),
                record_count INTEGER,
                loaded_at TIMESTAMP
            )
        """)

        cur.execute(
            """SELECT content_hash
               FROM raw.ingestion_batches
               WHERE batch_id = %s""",
            (LOTE,)
        )

        anteriores = cur.fetchall()

        if anteriores:
            if len(anteriores) != 1 or anteriores[0][0] != huella:
                raise ValueError(
                    "Lote existente con contenido diferente"
                )

            print("LOTE YA PROCESADO: sin duplicados")
            conexion.commit()

        else:
            for entidad, esperado in ENTIDADES.items():
                cur.execute(
                    f"DELETE FROM raw.{entidad} WHERE _batch_id = %s",
                    (LOTE,)
                )

                cur.execute(comandos[entidad])

                cur.execute(
                    f"""SELECT COUNT(*) FROM raw.{entidad}
                        WHERE _batch_id = %s""",
                    (LOTE,)
                )

                cargados = cur.fetchone()[0]

                if cargados != esperado:
                    raise ValueError(
                        f"{entidad}: esperado {esperado}, "
                        f"obtenido {cargados}"
                    )

                print(f"{entidad}: {cargados} cargados")

            cur.execute(
                """INSERT INTO raw.ingestion_batches
                   VALUES (%s, %s, %s, GETDATE())""",
                (LOTE, huella, total)
            )

            conexion.commit()
            print("CARGA COMPLETADA CORRECTAMENTE")

except Exception:
    conexion.rollback()
    print("ERROR: transaccion revertida")
    raise

finally:
    conexion.close()
