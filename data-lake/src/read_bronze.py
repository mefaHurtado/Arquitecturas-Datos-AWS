
import os
import argparse

import boto3
import feedparser


# Obtener configuración
BUCKET = os.environ["DATALAKE_BUCKET"]
REGION = os.environ.get("AWS_REGION", "us-east-1")

s3 = boto3.client("s3", region_name=REGION)

FUENTES = [
    "eltiempo_colombia",
    "eltiempo_economia",
    "eltiempo_tecnologia"
]


def leer_noticias(run_id):

    fecha = (
        f"{run_id[:4]}-"
        f"{run_id[4:6]}-"
        f"{run_id[6:8]}"
    )

    total = 0

    for fuente in FUENTES:

        ruta = (
            f"bronze/source={fuente}/"
            f"ingestion_date={fecha}/"
            f"run_id={run_id}/response.xml"
        )

        print(f"\n=== {fuente} ===")

        # Leer el XML directamente desde S3
        respuesta = s3.get_object(
            Bucket=BUCKET,
            Key=ruta
        )

        contenido = respuesta["Body"].read()

        # Interpretar el RSS
        feed = feedparser.parse(contenido)

        if feed.bozo:
            raise ValueError(
                f"RSS inválido en {fuente}: "
                f"{feed.bozo_exception}"
            )

        noticias = feed.entries

        print(f"Noticias encontradas: {len(noticias)}")

        total += len(noticias)

        # Inspeccionar la primera noticia
        if noticias:

            noticia = noticias[0]

            print("\nEjemplo de noticia:")
            print("ID:", noticia.get("id", "Sin ID"))
            print("Título:", noticia.get("title", ""))
            print("Enlace:", noticia.get("link", ""))
            print(
                "Fecha:",
                noticia.get("published", "Sin fecha")
            )
            print(
                "Descripción:",
                noticia.get("summary", "")[:200]
            )

    print("\n==============================")
    print(f"TOTAL ENTRADAS RSS: {total}")
    print("==============================")


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", required=True)

    args = parser.parse_args()

    leer_noticias(args.run_id)
