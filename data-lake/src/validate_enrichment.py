
from pathlib import Path
import pandas as pd

BASE = Path(__file__).resolve().parents[1]
RUTA = BASE / "local/comprehend_demo"

tablas = [
    "news",
    "news_entities",
    "news_key_phrases"
]

datos = {}

for nombre in tablas:
    archivo = RUTA / f"{nombre}.parquet"

    if not archivo.exists():
        raise SystemExit(f"Falta el archivo: {archivo}")

    datos[nombre] = pd.read_parquet(archivo)

news = datos["news"]
entities = datos["news_entities"]
phrases = datos["news_key_phrases"]

print("===== VALIDACIÓN COMPREHEND =====")

# 1. Verificar noticias únicas
assert not news.empty
assert news["news_id"].notna().all()
assert news["news_id"].is_unique

print("Noticias:", len(news))
print("Noticias duplicadas: 0")

# 2. Verificar integridad referencial
ids = set(news["news_id"])

assert set(entities["news_id"]).issubset(ids)
assert set(phrases["news_id"]).issubset(ids)

print("Entidades:", len(entities))
print("Frases clave:", len(phrases))
print("Relaciones con noticias: CORRECTAS")

# 3. Verificar ausencia de duplicados
assert not entities.duplicated(
    ["news_id", "entity_type", "entity_text"]
).any()

assert not phrases.duplicated(
    ["news_id", "phrase"]
).any()

print("Duplicados: 0")

# 4. Verificar confianza entre 0 y 1
for tabla in [entities, phrases]:
    assert tabla["confidence"].dropna().between(
        0, 1
    ).all()

print("Puntuaciones de confianza: CORRECTAS")

print("\nVALIDACIÓN EXITOSA")
print("Datos utilizados: demostración sintética")
