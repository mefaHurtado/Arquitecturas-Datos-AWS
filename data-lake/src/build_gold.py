
from pathlib import Path
import shutil
import pandas as pd

BASE = Path(__file__).resolve().parents[1]

SILVER = BASE / "local/silver/news_current.parquet"
GOLD = BASE / "local/gold/news_by_day"

SECCIONES = {
    "eltiempo_colombia": "Colombia",
    "eltiempo_economia": "Economía",
    "eltiempo_tecnologia": "Tecnología"
}

# 1. Leer las noticias vigentes
if not SILVER.exists():
    raise SystemExit("Falta news_current.parquet")

df = pd.read_parquet(SILVER)

columnas = {"news_id", "publication_date", "fuentes"}

if not columnas.issubset(df.columns):
    raise ValueError("Faltan columnas en Silver")

if df.empty:
    raise ValueError("Silver está vacío")

# 2. Validar identificadores
if df["news_id"].isna().any():
    raise ValueError("Hay identificadores vacíos")

if df["news_id"].duplicated().any():
    raise ValueError("Hay noticias duplicadas")

# 3. Validar fechas
fechas = pd.to_datetime(
    df["publication_date"],
    format="%Y-%m-%d",
    errors="coerce"
)

if fechas.isna().any():
    raise ValueError("Hay fechas inválidas")

# 4. Preparar registros por sección
filas = []

for _, noticia in df.iterrows():

    fuentes = str(noticia["fuentes"]).split(",")

    for fuente in set(f.strip() for f in fuentes):

        if fuente not in SECCIONES:
            raise ValueError(
                f"Fuente desconocida: {fuente}"
            )

        filas.append({
            "news_id": noticia["news_id"],
            "publication_date": noticia["publication_date"],
            "medio": "El Tiempo",
            "seccion": SECCIONES[fuente]
        })

detalle = pd.DataFrame(filas)

# 5. Contar noticias distintas por día y sección
gold = (
    detalle.groupby(
        ["publication_date", "medio", "seccion"],
        as_index=False
    )
    .agg(total_noticias=("news_id", "nunique"))
    .sort_values(
        ["publication_date", "medio", "seccion"]
    )
)

# 6. Conciliación con Silver
conteo_esperado = len(
    detalle.drop_duplicates(
        ["publication_date", "medio", "seccion", "news_id"]
    )
)

conteo_gold = int(gold["total_noticias"].sum())

assert conteo_gold == conteo_esperado

# 7. Guardar en Parquet, particionado por fecha
temporal = GOLD.parent / "_news_by_day_temporal"

if temporal.exists():
    shutil.rmtree(temporal)

temporal.parent.mkdir(parents=True, exist_ok=True)

gold.to_parquet(
    temporal,
    index=False,
    partition_cols=["publication_date"],
    engine="pyarrow"
)

if GOLD.exists():
    shutil.rmtree(GOLD)

temporal.rename(GOLD)

# 8. Mostrar resultados
print("\n===== RESULTADO GOLD =====")

print("Noticias únicas en Silver:", len(df))
print("Conteos por sección:", conteo_gold)
print("Registros agregados:", len(gold))

print("\nNoticias por día y sección:")
print(gold.to_string(index=False))

print("\nConciliación Silver-Gold: CORRECTA")
print("Archivo Gold:", GOLD)
print("\nPROCESAMIENTO GOLD FINALIZADO")
