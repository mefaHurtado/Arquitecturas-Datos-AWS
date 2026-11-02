import csv
from pathlib import Path
from decimal import Decimal
from collections import Counter

LOTE = "20261102T013140Z-c5e61618"
base = Path.home() / "dw-migracion-local"
entrada = base / "redshift-preparado" / LOTE
salida = base / "cuarentena-local"
salida.mkdir(parents=True, exist_ok=True)
salida.chmod(0o700)

tablas = [
    ("mysql_customers", "customer_id"),
    ("mysql_products", "product_id"),
    ("mysql_orders", "order_id"),
    ("mysql_order_items", "order_item_id"),
    ("mongo_reviews", "review_id"),
    ("mongo_navigation_events", "event_id")
]

datos = {}

def valor(fila, campo):
    x = fila.get(campo)
    return str(x).strip() if x is not None and str(x).strip() else None

def numero(fila, campo):
    x = valor(fila, campo)
    return Decimal(x) if x is not None else None

for tabla, clave in tablas:
    with (entrada / f"{tabla}.csv").open(
        encoding="utf-8", newline=""
    ) as f:
        datos[tabla] = list(csv.DictReader(f))

canales = {"web", "móvil", "tienda"}
eventos = {"vista_producto", "busqueda", "agregar_carrito"}

clientes_validos = {
    valor(r, "customer_id")
    for r in datos["mysql_customers"]
    if valor(r, "name")
    and valor(r, "email")
    and "@" in valor(r, "email")
}

productos_validos = {
    valor(r, "product_id")
    for r in datos["mysql_products"]
    if valor(r, "category")
    and (numero(r, "price") is None or numero(r, "price") >= 0)
}

def motivo(tabla, r):
    if tabla == "mysql_customers":
        if not valor(r, "name"):
            return "missing_name"
        if not valor(r, "email") or "@" not in valor(r, "email"):
            return "invalid_email"

    elif tabla == "mysql_products":
        if not valor(r, "category"):
            return "missing_category"
        if numero(r, "price") is not None and numero(r, "price") < 0:
            return "negative_price"

    elif tabla == "mysql_orders":
        if valor(r, "customer_id") not in clientes_validos:
            return "orphan_customer"
        if valor(r, "sales_channel") not in canales:
            return "invalid_sales_channel"
        if numero(r, "total") is not None and numero(r, "total") < 0:
            return "negative_total"

    elif tabla == "mysql_order_items":
        if valor(r, "order_id") not in pedidos_validos:
            return "orphan_order"
        if valor(r, "product_id") not in productos_validos:
            return "orphan_product"
        q = numero(r, "quantity")
        p = numero(r, "unit_price")
        s = numero(r, "subtotal")
        if q is not None and q <= 0:
            return "invalid_quantity"
        if None not in (q, p, s) and abs(s - p*q) > Decimal("0.01"):
            return "subtotal_mismatch"

    elif tabla == "mongo_reviews":
        rating = numero(r, "rating")
        if rating is None or not 1 <= rating <= 5:
            return "invalid_rating"
        if not valor(r, "reviewed_at"):
            return "missing_reviewed_at"
        if valor(r, "customer_id") not in clientes_validos:
            return "orphan_customer"
        if valor(r, "product_id") not in productos_validos:
            return "orphan_product"

    elif tabla == "mongo_navigation_events":
        if valor(r, "event_type") not in eventos:
            return "invalid_event_type"
        if not valor(r, "occurred_at"):
            return "missing_occurred_at"
        if valor(r, "product_id") not in productos_validos:
            return "orphan_product"
        c = valor(r, "customer_id")
        if c is not None and c not in clientes_validos:
            return "orphan_customer"
        if valor(r, "sales_channel") not in canales:
            return "invalid_sales_channel"

    return None

pedidos_validos = {
    valor(r, "order_id")
    for r in datos["mysql_orders"]
    if motivo("mysql_orders", r) is None
}

rechazos = []
total = 0

for tabla, clave in tablas:
    for fila in datos[tabla]:
        total += 1
        razon = motivo(tabla, fila)
        if razon:
            rechazos.append({
                "entidad": tabla,
                "record_id": valor(fila, clave) or "",
                "rejection_reason": razon,
                "batch_id": valor(fila, "_batch_id") or LOTE
            })

archivo = salida / f"rechazos_{LOTE}.csv"

with archivo.open("w", newline="", encoding="utf-8") as f:
    escritor = csv.DictWriter(
        f, fieldnames=[
            "entidad", "record_id",
            "rejection_reason", "batch_id"
        ]
    )
    escritor.writeheader()
    escritor.writerows(rechazos)

archivo.chmod(0o600)

print("=== RESUMEN DE CUARENTENA LOCAL ===")
print("Registros analizados:", total)
print("Registros rechazados:", len(rechazos))
print("Registros sin rechazo:", total - len(rechazos))

print("\n=== RECHAZOS POR ENTIDAD Y MOTIVO ===")
for (entidad, razon), cantidad in sorted(
    Counter(
        (r["entidad"], r["rejection_reason"])
        for r in rechazos
    ).items()
):
    print(f"{entidad} | {razon} | {cantidad}")

print("\n=== EJEMPLOS CON TRAZABILIDAD ===")
for r in rechazos[:12]:
    print(
        r["entidad"], "|",
        r["record_id"], "|",
        r["rejection_reason"], "|",
        r["batch_id"]
    )

print("\nArchivo generado:", archivo)
print("Simulacion local completada. No se enviaron datos a AWS.")
