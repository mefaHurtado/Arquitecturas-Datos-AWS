

import json
import hashlib
import xml.etree.ElementTree as ET

from pathlib import Path
from datetime import datetime, timedelta, timezone

from airflow.sdk import dag, task, Param, get_current_context


# Rutas de trabajo
BRONZE = Path("/opt/airflow/datalake-local/bronze")

# Carpeta exclusiva para el DAG de recuperacion
REPORTES = Path(
    "/opt/airflow/datalake-local/airflow_reports/recuperacion"
)

FUENTES = [
    "eltiempo_colombia",
    "eltiempo_economia",
    "eltiempo_tecnologia"
]


@dag(
    dag_id="eia_datalake_recuperacion",
    start_date=datetime(2026, 11, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    params={
        "source_run_id": Param("", type="string"),
        "fail_once": Param(False, type="boolean")
    },
    tags=["EIA", "Data Lake", "Recuperacion"]
)
def pipeline():

    # TAREA 1: Seleccionar el lote
    @task
    def seleccionar_lote():

        solicitado = get_current_context()["params"][
            "source_run_id"
        ]

        lotes_por_fuente = []

        for fuente in FUENTES:

            rutas = BRONZE.glob(
                f"source={fuente}/ingestion_date=*/"
                "run_id=*/manifest.json"
            )

            ids = {
                ruta.parent.name.replace("run_id=", "")
                for ruta in rutas
            }

            lotes_por_fuente.append(ids)

        completos = set.intersection(*lotes_por_fuente)

        if not completos:
            raise ValueError(
                "No hay lotes completos en Bronze"
            )

        if solicitado:

            if solicitado not in completos:
                raise ValueError(
                    f"Lote no disponible: {solicitado}"
                )

            lote = solicitado

        else:
            lote = sorted(completos)[-1]

        print("LOTE SELECCIONADO:", lote)

        return lote

    # TAREA 2: Validar los datos de Bronze
    @task
    def validar_datos(lote):

        total = 0
        enlaces = set()

        for fuente in FUENTES:

            rutas = list(BRONZE.glob(
                f"source={fuente}/ingestion_date=*/"
                f"run_id={lote}/manifest.json"
            ))

            if len(rutas) != 1:
                raise ValueError(
                    f"Manifiesto faltante o ambiguo: {fuente}"
                )

            manifiesto = json.loads(
                rutas[0].read_text(encoding="utf-8")
            )

            if manifiesto["status"] != "success":
                raise ValueError(
                    f"Fuente no exitosa: {fuente}"
                )

            xml = rutas[0].parent / "response.xml"

            contenido = xml.read_bytes()

            sha = hashlib.sha256(
                contenido
            ).hexdigest()

            if sha != manifiesto["sha256"]:
                raise ValueError(
                    f"Hash invalido en {fuente}"
                )

            raiz = ET.fromstring(contenido)

            noticias = raiz.findall("./channel/item")

            if len(noticias) != manifiesto["record_count"]:
                raise ValueError(
                    f"Conteo incorrecto en {fuente}"
                )

            for noticia in noticias:

                enlace = (
                    noticia.findtext("link") or ""
                ).strip()

                if not enlace:
                    raise ValueError(
                        f"Noticia sin enlace en {fuente}"
                    )

                enlaces.add(enlace)

            total += len(noticias)

            print(
                f"{fuente}: {len(noticias)} registros"
            )

        resultado = {
            "source_run_id": lote,
            "total": total,
            "unicas_por_enlace": len(enlaces),
            "duplicadas_por_enlace": total - len(enlaces),
            "status": "success"
        }

        print("VALIDACION CORRECTA")

        return resultado

    # TAREA 3: Probar recuperacion ante fallos
    @task(
        retries=1,
        retry_delay=timedelta(seconds=20)
    )
    def probar_recuperacion(resultado):

        contexto = get_current_context()

        provocar_fallo = contexto["params"][
            "fail_once"
        ]

        REPORTES.mkdir(
            parents=True,
            exist_ok=True
        )

        identificador = hashlib.sha256(
            contexto["run_id"].encode()
        ).hexdigest()[:16]

        marcador = REPORTES / (
            f"fallo_{identificador}.txt"
        )

        if provocar_fallo and not marcador.exists():

            marcador.write_text(
                "Fallo controlado ejecutado",
                encoding="utf-8"
            )

            raise RuntimeError(
                "FALLO CONTROLADO: "
                "se realizara un reintento"
            )

        print("RECUPERACION EXITOSA")

        return resultado

    # TAREA 4: Registrar los resultados
    @task
    def registrar_resultado(resultado):

        REPORTES.mkdir(
            parents=True,
            exist_ok=True
        )

        ruta = REPORTES / (
            resultado["source_run_id"] + ".json"
        )

        contenido = json.dumps(
            resultado,
            indent=4,
            sort_keys=True
        )

        if ruta.exists():

            existente = json.loads(
                ruta.read_text(encoding="utf-8")
            )

            if existente != resultado:
                raise ValueError(
                    "El resultado existente es diferente"
                )

            print(
                "LOTE YA PROCESADO SIN CAMBIOS"
            )

        else:

            temporal = ruta.with_suffix(
                ".json.tmp"
            )

            temporal.write_text(
                contenido,
                encoding="utf-8"
            )

            temporal.replace(ruta)

            print(
                "NUEVO REPORTE GUARDADO"
            )

        print("PIPELINE FINALIZADO")
        print("Reporte:", ruta)

    # Orden de ejecucion del DAG
    lote = seleccionar_lote()

    datos = validar_datos(lote)

    recuperado = probar_recuperacion(datos)

    registrar_resultado(recuperado)


pipeline()



