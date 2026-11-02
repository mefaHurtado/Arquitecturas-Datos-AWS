import csv
import json
import hashlib
import sqlite3
from decimal import Decimal
from pathlib import Path

LOTE = "20261102T013140Z-c5e61618"
INCREMENTAL = LOTE + "-incremental"

carpeta = (
    Path.home() / "dw-migracion-local"
    / "redshift-carga" / LOTE / "datos"
)

claves = {
    "mysql_customers": "customer_id",
    "mysql_products": "product_id",
    "mysql_orders": "order_id",
    "mysql_order_items": "order_item_id",
    "mongo_reviews": "review_id",
    "mongo_navigation_events": "event_id"
}

inicial = {}

for entidad in claves:
    with (carpeta / f"{entidad}.csv").open(
        encoding="utf-8", newline=""
    ) as archivo:
        inicial[entidad] = list(csv.DictReader(archivo))

db = sqlite3.connect(":memory:")

db.executescript("""
CREATE TABLE registros (
    entidad TEXT NOT NULL,
    registro_id TEXT NOT NULL,
    contenido TEXT NOT NULL,
    PRIMARY KEY (entidad, registro_id)
);

CREATE TABLE lotes (
    lote TEXT PRIMARY KEY,
    huella TEXT NOT NULL
);
""")

def total():
    return db.execute(
        "SELECT COUNT(*) FROM registros"
    ).fetchone()[0]

def cargar(lote, datos):
    contenido = json.dumps(
        datos, sort_keys=True, ensure_ascii=False
    )
    huella = hashlib.sha256(
        contenido.encode("utf-8")
    ).hexdigest()

    anterior = db.execute(
        "SELECT huella FROM lotes WHERE lote = ?",
        (lote,)
    ).fetchone()

    if anterior:
        if anterior[0] != huella:
            raise ValueError("Lote repetido con datos diferentes")
        return 0, 0, "LOTE YA PROCESADO"

    insertados = 0
    actualizados = 0

    with db:
        for entidad, filas in datos.items():
            clave = claves[entidad]
            ids_lote = set()

            for fila in filas:
                identificador = str(fila.get(clave) or "").strip()

                if not identificador or identificador in ids_lote:
                    raise ValueError("ID vacio o repetido en el lote")

                ids_lote.add(identificador)

                nuevo = json.dumps(
                    fila, sort_keys=True, ensure_ascii=False
                )

                anterior = db.execute(
                    """SELECT contenido FROM registros
                       WHERE entidad = ? AND registro_id = ?""",
                    (entidad, identificador)
                ).fetchone()

                if anterior is None:
                    db.execute(
                        "INSERT INTO registros VALUES (?, ?, ?)",
                        (entidad, identificador, nuevo)
                    )
                    insertados += 1

                elif anterior[0] != nuevo:
                    db.execute(
                        """UPDATE registros SET contenido = ?
                           WHERE entidad = ? AND registro_id = ?""",
                        (nuevo, entidad, identificador)
                    )
                    actualizados += 1

        db.execute(
            "INSERT INTO lotes VALUES (?, ?)",
            (lote, huella)
        )

    return insertados, actualizados, "PROCESADO"

print("=== CARGA INICIAL ===")
a, b, estado = cargar(LOTE, inicial)
print("Insertados:", a)
print("Actualizados:", b)
print("Total:", total())

# Preparar tres registros nuevos y actualizar un producto.
cliente = next(
    r for r in inicial["mysql_customers"]
    if r.get("name") and "@" in (r.get("email") or "")
)

producto = next(
    r for r in inicial["mysql_products"]
    if r.get("category")
    and Decimal(r["price"]) >= 0
)

pedido = inicial["mysql_orders"][0]
detalle = inicial["mysql_order_items"][0]

nuevo_cliente = dict(cliente)
nuevo_cliente.update(
    customer_id="900001",
    name="Cliente prueba incremental",
    email="incremental@example.invalid",
    _batch_id=INCREMENTAL
)

nuevo_pedido = dict(pedido)
nuevo_pedido.update(
    order_id="900002",
    customer_id="900001",
    sales_channel="web",
    total="150.00",
    _batch_id=INCREMENTAL
)

nuevo_detalle = dict(detalle)
nuevo_detalle.update(
    order_item_id="900003",
    order_id="900002",
    product_id=producto["product_id"],
    quantity="1",
    unit_price="150.00",
    subtotal="150.00",
    _batch_id=INCREMENTAL
)

producto_actualizado = dict(producto)
producto_actualizado["price"] = str(
    Decimal(producto["price"]) + Decimal("1.00")
)
producto_actualizado["_batch_id"] = INCREMENTAL

for entidad, identificador in [
    ("mysql_customers", "900001"),
    ("mysql_orders", "900002"),
    ("mysql_order_items", "900003")
]:
    clave = claves[entidad]
    if any(r[clave] == identificador for r in inicial[entidad]):
        raise SystemExit("ID de prueba ya existente")

incremento = {
    "mysql_customers": [nuevo_cliente],
    "mysql_orders": [nuevo_pedido],
    "mysql_order_items": [nuevo_detalle],
    "mysql_products": [producto_actualizado]
}

print("\n=== CARGA INCREMENTAL ===")
a, b, estado = cargar(INCREMENTAL, incremento)
print("Registros nuevos:", a)
print("Registros actualizados:", b)
print("Total despues del incremento:", total())

if (a, b, total()) != (3, 1, 1115):
    raise SystemExit("ERROR EN CARGA INCREMENTAL")

print("\n=== REPETICION DEL LOTE INCREMENTAL ===")
a, b, estado = cargar(INCREMENTAL, incremento)
print("Estado:", estado)
print("Nuevos agregados:", a)
print("Actualizaciones adicionales:", b)
print("Total final:", total())

if (a, b, total()) != (0, 0, 1115):
    raise SystemExit("ERROR DE IDEMPOTENCIA INCREMENTAL")

print("\n=== RESULTADO FINAL ===")
print("Carga inicial: 1112")
print("Nuevos registros: 3")
print("Registros actualizados: 1")
print("Total esperado: 1115")
print("Total obtenido:", total())
print("Duplicados por repeticion: 0")
print("PRUEBA INCREMENTAL: EXITOSA")
print("Simulacion local. No se enviaron datos a AWS.")
