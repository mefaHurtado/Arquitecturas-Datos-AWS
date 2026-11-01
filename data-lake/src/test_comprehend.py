
import os
import json
import hashlib
import argparse
from pathlib import Path
from datetime import datetime, timezone

import boto3
import pandas as pd

BASE = Path(__file__).resolve().parents[1]
ARCHIVO = BASE / "local/silver/news_current.parquet"
CACHE = BASE / "local/comprehend_cache"

CACHE.mkdir(parents=True, exist_ok=True)

parser = argparse.ArgumentParser()
parser.add_argument("--execute", action="store_true")
args = parser.parse_args()

# Leer noticias vigentes
df = pd.read_parquet(ARCHIVO)
df = df.sort_values("news_id").fillna("")

# Seleccionar una noticia con texto válido
seleccion = None

for _, noticia in df.iterrows():
    texto = (
        str(noticia["titulo"])
        + ". "
        + str(noticia["descripcion"])
    ).strip()

    if texto and len(texto.encode("utf-8")) <= 5000:
        seleccion = (noticia, texto)
        break

if seleccion is None:
    raise SystemExit("No hay textos válidos para analizar")

noticia, texto = seleccion

hash_texto = hashlib.sha256(
    texto.encode("utf-8")
).hexdigest()

ruta_cache = CACHE / f"{hash_texto}_es.json"

print("\n===== AMAZON COMPREHEND =====")
print("Noticia:", noticia["titulo"])
print("Idioma utilizado: es")
print("Bytes:", len(texto.encode("utf-8")))

if ruta_cache.exists():
    resultado = json.loads(
        ruta_cache.read_text(encoding="utf-8")
    )
    print("Resultado recuperado de caché")

elif not args.execute:
    print("\nMODO PRUEBA: no se realizaron llamadas AWS")
    print("Para analizar: python src/test_comprehend.py --execute")
    raise SystemExit(0)

else:
    comprehend = boto3.client(
        "comprehend",
        region_name=os.getenv("AWS_REGION", "us-east-1")
    )

    entidades = comprehend.detect_entities(
        Text=texto,
        LanguageCode="es"
    )

    frases = comprehend.detect_key_phrases(
        Text=texto,
        LanguageCode="es"
    )

    resultado = {
        "news_id": noticia["news_id"],
        "text": texto,
        "text_hash": hash_texto,
        "language": "es",
        "analyzed_at_utc": datetime.now(
            timezone.utc
        ).isoformat(),
        "entities": entidades["Entities"],
        "key_phrases": frases["KeyPhrases"]
    }

    ruta_cache.write_text(
        json.dumps(
            resultado,
            indent=2,
            ensure_ascii=False
        ),
        encoding="utf-8"
    )

print("\nENTIDADES DETECTADAS:")

for entidad in resultado["entities"]:
    print(
        entidad["Type"],
        "-",
        entidad["Text"],
        "- Confianza:",
        round(entidad["Score"], 3)
    )

print("\nFRASES CLAVE:")

for frase in resultado["key_phrases"]:
    print(
        frase["Text"],
        "- Confianza:",
        round(frase["Score"], 3)
    )

print("\nAnálisis completado")
print("Caché:", ruta_cache)
