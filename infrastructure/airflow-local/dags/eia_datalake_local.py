
import json
import xml.etree.ElementTree as ET

from datetime import datetime, timedelta, timezone
from pathlib import Path

from airflow.sdk import dag, task


BRONZE = Path("/opt/airflow/datalake-local/bronze")
REPORTES = Path("/opt/airflow/datalake-local/airflow_reports")

FUENTES = {
    "eltiempo_colombia",
    "eltiempo_economia",
    "eltiempo_tecnologia"
}


@dag(
    dag_id="eia_datalake_local",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=1)
    },
    tags=["EIA", "Data Lake", "Local"]
)
def pipeline_datalake():

    @task
    def validar_bronze():

        lotes = {}

        for archivo in BRONZE.rglob("response.xml"):

            partes = archivo.parts

            fuente = next(
                p.replace("source=", "")
                for p in partes
                if p.startswith("source=")
            )

            lote = next(
                p.replace("run_id=", "")
                for p in partes
                if p.startswith("run_id=")
            )

            lotes.setdefault(lote, {})[fuente] = str(archivo)

        completos = {
            lote: archivos
            for lote, archivos in lotes.items()
            if FUENTES.issubset(archivos)
        }

        if not completos:
            raise ValueError("No existen lotes completos en Bronze")

        ultimo = sorted(completos)[-1]

        print("Lote seleccionado:", ultimo)
        print("Fuentes encontradas:", len(completos[ultimo]))

        return {
            "run_id": ultimo,
            "archivos": completos[ultimo]
        }

    @task
    def leer_noticias(datos):

        total = 0
        enlaces = []

        for fuente, ruta in datos["archivos"].items():

            raiz = ET.parse(ruta).getroot()
            noticias = raiz.findall("./channel/item")

            cantidad = len(noticias)

            if cantidad == 0:
                raise ValueError(
                    f"Fuente sin noticias: {fuente}"
                )

            print(f"{fuente}: {cantidad} noticias")

            total += cantidad

            for noticia in noticias:
                enlace = noticia.findtext("link")

                if not enlace or not enlace.strip():
                    raise ValueError(
                        f"Noticia sin enlace: {fuente}"
                    )

                enlaces.append(enlace.strip())

        return {
            "run_id": datos["run_id"],
            "total": total,
            "unicas": len(set(enlaces)),
            "duplicadas": total - len(set(enlaces))
        }

    @task
    def validar_calidad(resultado):

        assert resultado["total"] > 0
        assert resultado["unicas"] > 0

        assert (
            resultado["total"]
            == resultado["unicas"] + resultado["duplicadas"]
        )

        print("VALIDACIÓN EXITOSA")
        print("Registros:", resultado["total"])
        print("Noticias únicas por enlace:", resultado["unicas"])
        print("Duplicados:", resultado["duplicadas"])

        return resultado

    @task
    def registrar_resultado(resultado):

        REPORTES.mkdir(parents=True, exist_ok=True)

        archivo = REPORTES / (
            resultado["run_id"] + ".json"
        )

        resumen = {
            **resultado,
            "status": "success"
        }

        temporal = archivo.with_suffix(".json.tmp")

        temporal.write_text(
            json.dumps(resumen, indent=4),
            encoding="utf-8"
        )

        temporal.replace(archivo)

        print("PIPELINE COMPLETADO")
        print("Reporte:", archivo)

    archivos = validar_bronze()
    noticias = leer_noticias(archivos)
    calidad = validar_calidad(noticias)
    registrar_resultado(calidad)


pipeline_datalake()
