import csv
import json
from collections import Counter
from pathlib import Path

LOTE = "20261102T013140Z-c5e61618"
base = Path.home() / "dw-migracion-local/redshift-preparado" / LOTE

tablas = {
    "mysql_customers": ("customer_id", 54),
    "mysql_products": ("product_id", 27),
    "mysql_orders": ("order_id", 106),
    "mysql_order_items": ("order_item_id", 259),
    "mongo_reviews": ("review_id", 158),
    "mongo_navigation_events": ("event_id", 508)
}

datos = {}
problemas = 0

print("=== VALIDACION DE CALIDAD ===")

for tabla, (clave, esperado) in tablas.items():
    archivo = base / f"{tabla}.csv"

    with archivo.open(encoding="utf-8", newline="") as f:
        filas = list(csv.DictReader(f))

    if len(filas) != esperado:
        raise SystemExit(f"Cantidad inesperada en {tabla}")

    if not all(clave in fila for fila in filas):
        raise SystemExit(f"Falta columna {clave} en {tabla}")

    valores = [
        fila[clave].strip()
        for fila in filas if fila[clave] and fila[clave].strip()
    ]

    vacios = len(filas) - len(valores)
    duplicados = sum(
        n - 1 for n in Counter(valores).values() if n > 1
    )

    json_invalidos = 0
    if tabla.startswith("mongo_"):
        for fila in filas:
            try:
                json.loads(fila["document_json"])
            except (ValueError, KeyError, TypeError):
                json_invalidos += 1

    datos[tabla] = filas
    problemas += vacios + duplicados + json_invalidos

    print(f"\n{tabla}")
    print("Registros:", len(filas))
    print("IDs vacios:", vacios)
    print("IDs duplicados:", duplicados)
    print("JSON invalidos:", json_invalidos)

print("\n=== RELACIONES ===")

relaciones = [
    ("mysql_orders", "customer_id", "mysql_customers", "customer_id"),
    ("mysql_order_items", "order_id", "mysql_orders", "order_id"),
    ("mysql_order_items", "product_id", "mysql_products", "product_id"),
    ("mongo_reviews", "customer_id", "mysql_customers", "customer_id"),
    ("mongo_reviews", "product_id", "mysql_products", "product_id"),
    ("mongo_navigation_events", "customer_id", "mysql_customers", "customer_id"),
    ("mongo_navigation_events", "product_id", "mysql_products", "product_id")
]

for origen, columna, destino, clave in relaciones:
    validos = {
        fila[clave] for fila in datos[destino]
        if fila.get(clave)
    }

    huerfanos = sum(
        1 for fila in datos[origen]
        if fila.get(columna) and fila[columna] not in validos
    )

    print(f"{origen}.{columna}: {huerfanos} referencias sin relacion")
    problemas += huerfanos

print("\n=== RESULTADO ===")
print("Registros analizados:", sum(len(f) for f in datos.values()))
print("Incidencias detectadas:", problemas)
print("Revision local finalizada")
print("No se enviaron datos a AWS")
