import argparse
import csv
import hashlib
import json
import re
import shutil
import uuid
from datetime import datetime, timezone
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument("--source", choices=["mysql", "mongodb"], required=True)
parser.add_argument("--input-dir", required=True)
parser.add_argument(
    "--output-dir",
    default="~/dw-migracion-local/s3-preparado"
)
parser.add_argument("--batch-id")
args = parser.parse_args()

fecha = datetime.now(timezone.utc)
batch = args.batch_id or (
    fecha.strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:8]
)

if not re.fullmatch(r"[A-Za-z0-9_-]+", batch):
    raise SystemExit("Identificador de lote no valido")

entrada = Path(args.input_dir).expanduser()
salida = Path(args.output_dir).expanduser()
archivos = sorted(entrada.glob("*.csv"))

if not archivos:
    raise SystemExit("No se encontraron archivos CSV")

tareas = []

for archivo in archivos:
    entidad = archivo.stem
    prefijo = args.source + "_"

    if entidad.startswith(prefijo):
        entidad = entidad[len(prefijo):]

    if not re.fullmatch(r"[A-Za-z0-9_-]+", entidad):
        raise SystemExit("Nombre de entidad no valido")

    destino = (
        salida / "raw" / args.source / entidad
        / f"batch_id={batch}" / archivo.name
    )

    if destino.exists() or (destino.parent / "manifest.json").exists():
        raise SystemExit(f"El lote ya existe: {destino.parent}")

    tareas.append((archivo, destino, entidad))

print("=== PREPARACION DE LOTES PARA S3 ===")

for archivo, destino, entidad in tareas:
    destino.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(archivo, destino)

    with destino.open(newline="", encoding="utf-8-sig") as f:
        lector = csv.DictReader(f)
        if not lector.fieldnames:
            raise SystemExit(f"CSV sin encabezados: {archivo.name}")
        cantidad = sum(1 for _ in lector)

    metadata = {
        "source": args.source,
        "entity": entidad,
        "batch_id": batch,
        "extracted_at_utc": fecha.isoformat(),
        "record_count": cantidad,
        "sha256": hashlib.sha256(destino.read_bytes()).hexdigest(),
        "file": archivo.name
    }

    (destino.parent / "manifest.json").write_text(
        json.dumps(metadata, indent=2),
        encoding="utf-8"
    )

    print("Ruta S3 prevista:", destino.relative_to(salida))
    print("Registros:", cantidad)

print("Preparacion local completada. No se envio nada a AWS.")
