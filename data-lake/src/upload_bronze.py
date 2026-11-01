
import os
import json
import argparse
from pathlib import Path

import boto3
from botocore.exceptions import ClientError

BASE_DIR = Path(__file__).resolve().parents[1]
BRONZE_DIR = BASE_DIR / "local" / "bronze"

BUCKET = os.environ.get("DATALAKE_BUCKET")
REGION = os.environ.get("AWS_REGION", "us-east-1")

if not BUCKET:
    raise SystemExit("ERROR: falta DATALAKE_BUCKET")

s3 = boto3.client("s3", region_name=REGION)


def subir_archivo(key, contenido, tipo):
    try:
        s3.put_object(
            Bucket=BUCKET,
            Key=key,
            Body=contenido,
            ContentType=tipo,
            IfNoneMatch="*"
        )
        print(f"SUBIDO: {key}")

    except ClientError as error:
        codigo = error.response["Error"]["Code"]

        if codigo in ("PreconditionFailed", "412"):
            print(f"YA EXISTE: {key}")
        else:
            raise


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id")
    args = parser.parse_args()

    encontrados = 0

    for ruta_manifest in sorted(
        BRONZE_DIR.rglob("manifest.json")
    ):
        metadata = json.loads(
            ruta_manifest.read_text(encoding="utf-8")
        )

        if args.run_id and metadata["run_id"] != args.run_id:
            continue

        if metadata["status"] not in ("success", "empty"):
            print(f"OMITIDO: {metadata['source_id']}")
            continue

        ruta_xml = ruta_manifest.parent / "response.xml"

        if not ruta_xml.is_file():
            raise FileNotFoundError(ruta_xml)

        prefijo = (
            "bronze/"
            + ruta_manifest.parent.relative_to(
                BRONZE_DIR
            ).as_posix()
        )

        key_xml = f"{prefijo}/response.xml"
        key_manifest = f"{prefijo}/manifest.json"

        metadata["s3_original_path"] = (
            f"s3://{BUCKET}/{key_xml}"
        )
        metadata["s3_manifest_path"] = (
            f"s3://{BUCKET}/{key_manifest}"
        )

        subir_archivo(
            key_xml,
            ruta_xml.read_bytes(),
            "application/xml"
        )

        manifest_bytes = json.dumps(
            metadata,
            indent=4,
            ensure_ascii=False
        ).encode("utf-8")

        subir_archivo(
            key_manifest,
            manifest_bytes,
            "application/json"
        )

        ruta_manifest.write_bytes(manifest_bytes)
        encontrados += 1

    print(f"\nLotes revisados: {encontrados}")
    print("Carga a S3 finalizada.")


if __name__ == "__main__":
    main()
