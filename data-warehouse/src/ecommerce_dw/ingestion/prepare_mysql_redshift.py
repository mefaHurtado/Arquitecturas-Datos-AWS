import csv
import json
from pathlib import Path
from datetime import datetime, timezone

base = Path.home() / "dw-migracion-local"
lote = "20261102T013140Z-c5e61618"

origen = base / "extracciones-reales/mysql"
destino = base / "redshift-preparado" / lote
destino.mkdir(parents=True, exist_ok=True)

tablas = {
    "customers": 54,
    "products": 27,
    "orders": 106,
    "order_items": 259
}

total = 0
fecha = datetime.now(timezone.utc).isoformat()

print("=== PREPARANDO MYSQL PARA REDSHIFT ===")

for tabla, esperado in tablas.items():
    entrada = origen / f"mysql_{tabla}.csv"
    salida = destino / f"mysql_{tabla}.csv"

    with entrada.open(encoding="utf-8", newline="") as f:
        lector = csv.DictReader(f)
        columnas = lector.fieldnames or []
        filas = list(lector)

    if len(filas) != esperado:
        raise SystemExit(f"Conteo incorrecto: {tabla}")

    campos = columnas + [
        "_loaded_at", "_source", "_batch_id"
    ]

    with salida.open("w", encoding="utf-8", newline="") as f:
        escritor = csv.DictWriter(f, fieldnames=campos)
        escritor.writeheader()

        for fila in filas:
            fila["_loaded_at"] = fecha
            fila["_source"] = "mysql"
            fila["_batch_id"] = lote
            escritor.writerow(fila)

    salida.chmod(0o600)
    print(f"{tabla}: {len(filas)} registros OK")
    total += len(filas)

print("\n=== VERIFICANDO MONGODB ===")

for nombre, esperado in {
    "reviews": 158,
    "navigation_events": 508
}.items():
    archivo = destino / f"mongo_{nombre}.csv"

    with archivo.open(encoding="utf-8", newline="") as f:
        filas = list(csv.DictReader(f))

    if len(filas) != esperado:
        raise SystemExit(f"Conteo incorrecto: {nombre}")

    invalidos = sum(
        1 for fila in filas
        if not fila.get("document_json")
        or not json.loads(fila["document_json"])
    )

    print(f"{nombre}: {len(filas)} registros OK")
    print(f"Documentos JSON vacios: {invalidos}")
    total += len(filas)

print("\n=== RESULTADO FINAL ===")
print("Total de registros:", total)
print("Archivos preparados:", len(list(destino.glob("*.csv"))))
print("Carpeta:", destino)
print("No se enviaron datos a AWS")
