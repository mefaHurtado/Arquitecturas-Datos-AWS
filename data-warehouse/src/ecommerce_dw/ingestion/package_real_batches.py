import csv
import hashlib
import json
import shutil
import uuid
from datetime import datetime, timezone
from pathlib import Path

base = Path.home() / "dw-migracion-local"
entrada = base / "extracciones-reales"
salida = base / "s3-preparado"

archivos = {
    "mysql": {
        "customers": ("csv", 54),
        "products": ("csv", 27),
        "orders": ("csv", 106),
        "order_items": ("csv", 259),
    },
    "mongodb": {
        "navigation_events": ("jsonl", 508),
        "reviews": ("jsonl", 158),
    },
}

fecha = datetime.now(timezone.utc)
lote = fecha.strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:8]

verificados = []

print("=== VERIFICANDO DATOS REALES ===")

for fuente, entidades in archivos.items():
    for entidad, (formato, esperado) in entidades.items():
        origen = entrada / fuente / f"{fuente}_{entidad}.{formato}"

        if not origen.is_file():
            raise SystemExit(f"FALTA ARCHIVO: {origen}")

        with origen.open(encoding="utf-8", newline="") as f:
            if formato == "csv":
                lector = csv.DictReader(f)
                cantidad = sum(1 for fila in lector)
            else:
                cantidad = 0
                for linea in f:
                    if linea.strip():
                        json.loads(linea)
                        cantidad += 1

        if cantidad != esperado:
            raise SystemExit(
                f"ERROR {entidad}: {cantidad} registros, "
                f"se esperaban {esperado}"
            )

        verificados.append(
            (fuente, entidad, formato, origen, cantidad)
        )
        print(f"{fuente}/{entidad}: {cantidad} OK")

print("\n=== ORGANIZANDO LOTES ===")

for fuente, entidad, formato, origen, cantidad in verificados:
    carpeta = (
        salida / "raw" / fuente / entidad
        / f"batch_id={lote}"
    )
    carpeta.mkdir(parents=True, exist_ok=False)

    destino = carpeta / f"{entidad}.{formato}"

    with origen.open("rb") as src, destino.open("xb") as dst:
        shutil.copyfileobj(src, dst)

    metadata = {
        "source": fuente,
        "entity": entidad,
        "batch_id": lote,
        "extracted_at_utc": datetime.fromtimestamp(
            origen.stat().st_mtime, timezone.utc
        ).isoformat(),
        "packaged_at_utc": fecha.isoformat(),
        "record_count": cantidad,
        "format": formato,
        "sha256": hashlib.sha256(destino.read_bytes()).hexdigest(),
        "s3_key": str(destino.relative_to(salida))
    }

    with (carpeta / "manifest.json").open("x") as f:
        json.dump(metadata, f, indent=2)

    print(f"OK: {fuente}/{entidad}")

print("\n=== RESULTADO FINAL ===")
print("Lote:", lote)
print("Archivos de datos:", len(verificados))
print("Total de registros:", sum(x[4] for x in verificados))
print("Preparacion local completada.")
print("No se enviaron archivos a AWS.")
