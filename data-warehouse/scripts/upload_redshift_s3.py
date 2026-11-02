import argparse
import csv
import json
import re
import subprocess
from pathlib import Path
from urllib.parse import urlparse

LOTE = "20261102T013140Z-c5e61618"
BUCKET = "stefany-redshift-taller-2026"

base = (
    Path.home() / "dw-migracion-local"
    / "redshift-carga" / LOTE
)

sql = Path("data-warehouse/sql/copy_raw_redshift.sql")
contenido = sql.read_text(encoding="utf-8")

entidades = {
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

operaciones = []
total = 0

print("=== PREPARACION DE CARGA S3 PARA REDSHIFT ===")

for entidad, esperado in entidades.items():
    archivo = base / "datos" / f"{entidad}.csv"
    manifiesto = (
        base / "manifiestos" / f"{entidad}.manifest"
    )

    if not archivo.is_file() or not manifiesto.is_file():
        raise SystemExit(f"Faltan archivos: {entidad}")

    with archivo.open(encoding="utf-8", newline="") as f:
        filas = list(csv.DictReader(f))

    if len(filas) != esperado:
        raise SystemExit(f"Cantidad incorrecta: {entidad}")

    if any(fila.get("_batch_id") != LOTE for fila in filas):
        raise SystemExit(f"Lote incorrecto: {entidad}")

    contenido_manifest = json.loads(
        manifiesto.read_text(encoding="utf-8")
    )

    entradas = contenido_manifest.get("entries", [])

    if len(entradas) != 1:
        raise SystemExit(f"Manifiesto inesperado: {entidad}")

    url_datos = entradas[0]["url"]

    coincidencia = re.search(
        rf"\bCOPY\s+raw\.{entidad}\b(.*?);",
        contenido,
        re.IGNORECASE | re.DOTALL
    )

    if not coincidencia:
        raise SystemExit(f"Falta COPY: {entidad}")

    origen_copy = re.search(
        r"FROM\s+'([^']+)'",
        coincidencia.group(1),
        re.IGNORECASE
    )

    if not origen_copy:
        raise SystemExit(f"Falta FROM: {entidad}")

    url_manifiesto = origen_copy.group(1)

    for url, nombre in [
        (url_datos, f"{entidad}.csv"),
        (url_manifiesto, f"{entidad}.manifest")
    ]:
        parsed = urlparse(url)

        if (
            parsed.scheme != "s3"
            or parsed.netloc != BUCKET
            or not parsed.path.lstrip("/").startswith(
                "data-warehouse/"
            )
            or not parsed.path.endswith("/" + nombre)
        ):
            raise SystemExit(f"Ruta S3 incorrecta: {entidad}")

    operaciones.append((archivo, url_datos))
    operaciones.append((manifiesto, url_manifiesto))

    total += len(filas)
    print(f"{entidad}: {len(filas)} registros | LISTO")

if total != 1112 or len(operaciones) != 12:
    raise SystemExit("Cantidad total inesperada")

if len({url for _, url in operaciones}) != 12:
    raise SystemExit("Hay rutas S3 repetidas")

print("\n=== RESULTADO ===")
print("Archivos preparados:", len(operaciones))
print("Registros:", total)
print("Manifiestos:", len(entidades))

if not args.execute:
    print("\nSIMULACION EXITOSA")
    print("NO SE SUBIERON ARCHIVOS A AWS")
    raise SystemExit(0)

confirmacion = input(
    "Se subiran 12 archivos a S3. Escribe SUBIR: "
)

if confirmacion != "SUBIR":
    raise SystemExit("Operacion cancelada")

for archivo, destino in operaciones:
    subprocess.run(
        [
            "aws", "s3", "cp",
            str(archivo), destino,
            "--region", "us-east-1",
            "--sse", "AES256"
        ],
        check=True
    )

print("\nSUBIDA COMPLETADA")
