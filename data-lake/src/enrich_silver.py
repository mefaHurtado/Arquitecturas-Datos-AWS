
import argparse
import hashlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path

import boto3
import pandas as pd
from langdetect import (
    detect,
    DetectorFactory,
    LangDetectException
)

DetectorFactory.seed = 0

BASE = Path(__file__).resolve().parents[1]
ENTRADA = BASE / "local/silver/news_current.parquet"
SALIDA = BASE / "local/silver_enriched"
CACHE = BASE / "local/comprehend_cache_real"

SALIDA.mkdir(parents=True, exist_ok=True)
CACHE.mkdir(parents=True, exist_ok=True)

parser = argparse.ArgumentParser()
parser.add_argument("--execute", action="store_true")
parser.add_argument("--limit", type=int, default=3)
args = parser.parse_args()

if args.limit < 0:
    raise SystemExit("El límite no puede ser negativo")

df = pd.read_parquet(ENTRADA)

if df.empty or df["news_id"].duplicated().any():
    raise ValueError("Silver vacío o con IDs duplicados")

df = df.sort_values("news_id").reset_index(drop=True)

for columna in [
    "language",
    "enrichment_status",
    "analysis_hash",
    "analyzed_at_utc"
]:
    df[columna] = ""

df["enrichment_status"] = "pending"

cliente = boto3.client(
    "comprehend",
    region_name=os.getenv("AWS_REGION", "us-east-1")
) if args.execute else None

entidades = []
frases = []
seleccionadas = len(df) if args.limit == 0 else min(
    args.limit, len(df)
)

for i in range(seleccionadas):
    noticia = df.iloc[i]
    news_id = noticia["news_id"]

    titulo = str(noticia["titulo"] or "")
    descripcion = str(noticia["descripcion"] or "")
    texto = (titulo + ". " + descripcion).strip()

    tamaño = len(texto.encode("utf-8"))

    if tamaño < 20:
        df.at[i, "enrichment_status"] = "empty_or_short"
        continue

    if tamaño > 5000:
        df.at[i, "enrichment_status"] = "too_long"
        continue

    try:
        idioma = detect(texto)
    except LangDetectException:
        df.at[i, "enrichment_status"] = "unknown_language"
        continue

    df.at[i, "language"] = idioma

    if idioma != "es":
        df.at[i, "enrichment_status"] = "unsupported_language"
        continue

    hash_texto = hashlib.sha256(
        texto.encode("utf-8")
    ).hexdigest()

    df.at[i, "analysis_hash"] = hash_texto

    ruta_cache = CACHE / f"{hash_texto}_es.json"

    if ruta_cache.exists():
        resultado = json.loads(
            ruta_cache.read_text(encoding="utf-8")
        )
        print("CACHÉ:", news_id[:12])

    elif args.execute:
        respuesta_entidades = cliente.detect_entities(
            Text=texto,
            LanguageCode="es"
        )

        respuesta_frases = cliente.detect_key_phrases(
            Text=texto,
            LanguageCode="es"
        )

        resultado = {
            "text": texto,
            "text_hash": hash_texto,
            "language": "es",
            "analyzed_at_utc": datetime.now(
                timezone.utc
            ).isoformat(),
            "entities": respuesta_entidades["Entities"],
            "key_phrases": respuesta_frases["KeyPhrases"]
        }

        ruta_cache.write_text(
            json.dumps(
                resultado,
                ensure_ascii=False,
                indent=2
            ),
            encoding="utf-8"
        )

        print("ANALIZADA:", news_id[:12])

    else:
        print("PENDIENTE:", news_id[:12])
        continue

    df.at[i, "enrichment_status"] = "completed"
    df.at[i, "analyzed_at_utc"] = resultado[
        "analyzed_at_utc"
    ]

    # Eliminar entidades repetidas de cada noticia
    entidades_unicas = {}

    for e in resultado["entities"]:
        if e["Type"] not in (
            "PERSON", "ORGANIZATION", "LOCATION"
        ):
            continue

        clave = (
            e["Type"],
            e["Text"].strip().casefold()
        )

        entidades_unicas[clave] = max(
            entidades_unicas.get(clave, 0),
            float(e["Score"])
        )

    for (tipo, nombre), confianza in entidades_unicas.items():
        entidades.append({
            "news_id": news_id,
            "entity_type": tipo,
            "entity_text": nombre,
            "confidence": confianza,
            "analysis_hash": hash_texto
        })

    # Eliminar frases repetidas de cada noticia
    frases_unicas = {}

    for f in resultado["key_phrases"]:
        nombre = f["Text"].strip().casefold()

        if nombre:
            frases_unicas[nombre] = max(
                frases_unicas.get(nombre, 0),
                float(f["Score"])
            )

    for nombre, confianza in frases_unicas.items():
        frases.append({
            "news_id": news_id,
            "phrase": nombre,
            "confidence": confianza,
            "analysis_hash": hash_texto
        })

if args.execute:
    df.to_parquet(SALIDA / "news.parquet", index=False)

    entidades_df = pd.DataFrame(
        entidades,
        columns=[
            "news_id", "entity_type",
            "entity_text", "confidence", "analysis_hash"
        ]
    ).astype({
        "news_id": "string",
        "entity_type": "string",
        "entity_text": "string",
        "confidence": "float64",
        "analysis_hash": "string"
    })

    frases_df = pd.DataFrame(
        frases,
        columns=[
            "news_id", "phrase",
            "confidence", "analysis_hash"
        ]
    ).astype({
        "news_id": "string",
        "phrase": "string",
        "confidence": "float64",
        "analysis_hash": "string"
    })

    entidades_df.to_parquet(
        SALIDA / "news_entities.parquet",
        index=False
    )

    frases_df.to_parquet(
        SALIDA / "news_key_phrases.parquet",
        index=False
    )

print("\n===== RESULTADO COMPREHEND =====")
print("Noticias vigentes:", len(df))
print("Noticias seleccionadas:", seleccionadas)
print("Estados:")
print(df["enrichment_status"].value_counts())
print("Entidades:", len(entidades))
print("Frases clave:", len(frases))

if args.execute:
    print("Archivos Parquet guardados en:", SALIDA)
else:
    print("Simulación: no se realizaron llamadas a AWS")
