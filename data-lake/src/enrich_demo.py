
import argparse
import hashlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path

import boto3
import pandas as pd

BASE = Path(__file__).resolve().parents[1]
SALIDA = BASE / "local" / "comprehend_demo"
CACHE = BASE / "local" / "comprehend_cache"

SALIDA.mkdir(parents=True, exist_ok=True)
CACHE.mkdir(parents=True, exist_ok=True)

# Textos originales ficticios para el laboratorio
NOTICIAS = [
    {
        "news_id": "demo_001",
        "titulo": "Proyecto educativo en Medellín",
        "descripcion": (
            "La Fundación Horizonte presentó en Medellín "
            "un programa de formación tecnológica."
        )
    },
    {
        "news_id": "demo_002",
        "titulo": "Encuentro de tecnología en Bogotá",
        "descripcion": (
            "Carlos Pérez participó en Bogotá en un "
            "encuentro organizado por Innovación Colombia."
        )
    },
    {
        "news_id": "demo_003",
        "titulo": "Investigación ambiental en Cali",
        "descripcion": (
            "La Universidad Aurora anunció en Cali "
            "una investigación sobre sostenibilidad."
        )
    }
]


def guardar_parquet(nombre, filas, columnas):
    df = pd.DataFrame(filas, columns=columnas)
    df.to_parquet(SALIDA / nombre, index=False)
    print(f"Generado: {nombre} | Filas: {len(df)}")


def ejecutar():
    parser = argparse.ArgumentParser()
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--limit", type=int, default=1)
    args = parser.parse_args()

    if not 1 <= args.limit <= len(NOTICIAS):
        raise SystemExit("El límite debe estar entre 1 y 3")

    seleccion = NOTICIAS[:args.limit]

    if not args.execute:
        print("MODO PRUEBA: sin llamadas a AWS")
        print("Noticias seleccionadas:", len(seleccion))
        print("Idioma: es")
        print("Para analizar, utiliza --execute")
        return

    cliente = boto3.client(
        "comprehend",
        region_name=os.getenv("AWS_REGION", "us-east-1")
    )

    news = []
    entidades = []
    frases = []

    for noticia in seleccion:
        texto = (
            noticia["titulo"] + ". "
            + noticia["descripcion"]
        )

        text_hash = hashlib.sha256(
            texto.encode("utf-8")
        ).hexdigest()

        archivo_cache = CACHE / f"{text_hash}_es.json"

        if archivo_cache.exists():
            resultado = json.loads(
                archivo_cache.read_text(encoding="utf-8")
            )
            print("CACHÉ:", noticia["news_id"])
        else:
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
                "text_hash": text_hash,
                "language": "es",
                "analyzed_at_utc": datetime.now(
                    timezone.utc
                ).isoformat(),
                "entities": respuesta_entidades["Entities"],
                "key_phrases": respuesta_frases["KeyPhrases"]
            }

            archivo_cache.write_text(
                json.dumps(
                    resultado,
                    ensure_ascii=False,
                    indent=2
                ),
                encoding="utf-8"
            )

            print("ANALIZADA:", noticia["news_id"])

        news.append({
            **noticia,
            "text_hash": text_hash,
            "language": "es",
            "enrichment_status": "completed",
            "analyzed_at_utc": resultado["analyzed_at_utc"]
        })

        # Una fila por entidad distinta en la noticia
        unicas = {}

        for entidad in resultado["entities"]:
            if entidad["Type"] not in (
                "PERSON", "ORGANIZATION", "LOCATION"
            ):
                continue

            clave = (
                entidad["Type"],
                entidad["Text"].strip().casefold()
            )

            unicas[clave] = max(
                unicas.get(clave, 0),
                float(entidad["Score"])
            )

        for (tipo, nombre), confianza in unicas.items():
            entidades.append({
                "news_id": noticia["news_id"],
                "entity_type": tipo,
                "entity_text": nombre,
                "confidence": confianza,
                "text_hash": text_hash
            })

        # Una fila por frase distinta en la noticia
        frases_unicas = {}

        for frase in resultado["key_phrases"]:
            nombre = frase["Text"].strip().casefold()

            if nombre:
                frases_unicas[nombre] = max(
                    frases_unicas.get(nombre, 0),
                    float(frase["Score"])
                )

        for nombre, confianza in frases_unicas.items():
            frases.append({
                "news_id": noticia["news_id"],
                "phrase": nombre,
                "confidence": confianza,
                "text_hash": text_hash
            })

    guardar_parquet(
        "news.parquet",
        news,
        [
            "news_id", "titulo", "descripcion",
            "text_hash", "language",
            "enrichment_status", "analyzed_at_utc"
        ]
    )

    guardar_parquet(
        "news_entities.parquet",
        entidades,
        [
            "news_id", "entity_type",
            "entity_text", "confidence", "text_hash"
        ]
    )

    guardar_parquet(
        "news_key_phrases.parquet",
        frases,
        [
            "news_id", "phrase",
            "confidence", "text_hash"
        ]
    )

    print("\nENRIQUECIMIENTO DEMO COMPLETADO")


if __name__ == "__main__":
    ejecutar()
