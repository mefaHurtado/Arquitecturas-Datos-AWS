
import os
import json
import hashlib
import argparse
from pathlib import Path
from datetime import datetime, timezone
from urllib.parse import urlsplit, urlunsplit

import boto3
import pandas as pd
import feedparser
from bs4 import BeautifulSoup


# Configuración
BASE_DIR = Path(__file__).resolve().parents[1]
BUCKET = os.environ["DATALAKE_BUCKET"]
REGION = os.environ.get("AWS_REGION", "us-east-1")

s3 = boto3.client("s3", region_name=REGION)

FUENTES = [
    "eltiempo_colombia",
    "eltiempo_economia",
    "eltiempo_tecnologia"
]


def limpiar_html(texto):
    return BeautifulSoup(
        str(texto or ""), "html.parser"
    ).get_text(" ", strip=True)


def normalizar_fecha(noticia):
    original = (
        noticia.get("published")
        or noticia.get("updated")
        or ""
    )

    fecha = (
        noticia.get("published_parsed")
        or noticia.get("updated_parsed")
    )

    if not fecha:
        return original, None

    fecha_utc = datetime(
        *fecha[:6],
        tzinfo=timezone.utc
    )

    return original, fecha_utc.isoformat()


def validar_url(enlace):
    partes = urlsplit(enlace.strip())

    if partes.scheme.lower() not in ("http", "https"):
        return None

    if not partes.netloc:
        return None

    return urlunsplit((
        partes.scheme.lower(),
        partes.netloc.lower(),
        partes.path or "/",
        partes.query,
        ""
    ))


def procesar(run_id):
    fecha_lote = (
        f"{run_id[:4]}-"
        f"{run_id[4:6]}-"
        f"{run_id[6:8]}"
    )

    noticias = {}
    rechazados = []
    recibidos = 0
    duplicados = 0

    for fuente in FUENTES:
        key = (
            f"bronze/source={fuente}/"
            f"ingestion_date={fecha_lote}/"
            f"run_id={run_id}/response.xml"
        )

        print(f"\nProcesando: {fuente}")

        respuesta = s3.get_object(
            Bucket=BUCKET,
            Key=key
        )

        contenido = respuesta["Body"].read()
        feed = feedparser.parse(contenido)

        if feed.bozo or not feed.version:
            raise ValueError(
                f"RSS inválido: {key}"
            )

        for entrada in feed.entries:
            recibidos += 1

            titulo = limpiar_html(
                entrada.get("title", "")
            )

            descripcion = limpiar_html(
                entrada.get("summary", "")
            )

            enlace = entrada.get("link", "").strip()
            url_normalizada = validar_url(enlace)

            fecha_original, fecha_utc = (
                normalizar_fecha(entrada)
            )

            motivos = []

            if not titulo:
                motivos.append("titulo_vacio")

            if not url_normalizada:
                motivos.append("enlace_invalido")

            if not fecha_utc:
                motivos.append("fecha_invalida")

            if motivos:
                rechazados.append({
                    "source_id": fuente,
                    "run_id": run_id,
                    "bronze_s3_key": key,
                    "titulo": titulo,
                    "enlace": enlace,
                    "motivo": motivos
                })
                continue

            # Identificador estable basado en enlace
            news_id = hashlib.sha256(
                url_normalizada.encode("utf-8")
            ).hexdigest()

            texto = titulo + "|" + descripcion

            text_hash = hashlib.sha256(
                texto.encode("utf-8")
            ).hexdigest()

            if news_id in noticias:
                duplicados += 1

                fuentes_actuales = set(
                    noticias[news_id]["fuentes"].split(",")
                )
                fuentes_actuales.add(fuente)

                noticias[news_id]["fuentes"] = ",".join(
                    sorted(fuentes_actuales)
                )
                continue

            noticias[news_id] = {
                "news_id": news_id,
                "titulo": titulo,
                "descripcion": descripcion,
                "enlace": enlace,
                "fecha_original": fecha_original,
                "published_at_utc": fecha_utc,
                "publication_date": fecha_utc[:10],
                "fuentes": fuente,
                "text_hash": text_hash,
                "run_id": run_id,
                "bronze_s3_key": key
            }

    # Guardar resultados locales de este lote
    silver_dir = (
        BASE_DIR / "local" / "silver_batches"
        / f"run_id={run_id}"
    )

    silver_dir.mkdir(
        parents=True, exist_ok=True
    )

    df = pd.DataFrame(list(noticias.values()))

    columnas = [
        "news_id", "titulo", "descripcion",
        "enlace", "fecha_original",
        "published_at_utc", "publication_date",
        "fuentes", "text_hash",
        "run_id", "bronze_s3_key"
    ]

    df = df.reindex(columns=columnas)

    df.to_parquet(
        silver_dir / "news.parquet",
        index=False
    )

    quarantine_dir = (
        BASE_DIR / "local" / "quarantine"
        / f"run_id={run_id}"
    )

    quarantine_dir.mkdir(
        parents=True, exist_ok=True
    )

    with open(
        quarantine_dir / "invalid.json",
        "w",
        encoding="utf-8"
    ) as archivo:
        json.dump(
            rechazados,
            archivo,
            ensure_ascii=False,
            indent=2
        )

    print("\n===== RESULTADO SILVER =====")
    print("Recibidos:", recibidos)
    print("Únicos válidos:", len(df))
    print("Duplicados:", duplicados)
    print("Rechazados:", len(rechazados))
    print("Archivo:", silver_dir / "news.parquet")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", required=True)
    args = parser.parse_args()

    procesar(args.run_id)
