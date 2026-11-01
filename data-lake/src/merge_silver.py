
from pathlib import Path
import pandas as pd

# Rutas del proyecto
BASE_DIR = Path(__file__).resolve().parents[1]

ENTRADA = BASE_DIR / "local" / "silver_batches"
SALIDA = BASE_DIR / "local" / "silver"

SALIDA.mkdir(parents=True, exist_ok=True)

# Buscar los lotes procesados
archivos = sorted(
    ENTRADA.glob("run_id=*/news.parquet")
)

if len(archivos) < 2:
    raise SystemExit(
        "Se necesitan al menos dos lotes Silver."
    )

tablas = []

# Leer cada lote
for archivo in archivos:
    lote = archivo.parent.name.replace("run_id=", "")

    df = pd.read_parquet(archivo)
    df["run_id"] = lote

    if df["news_id"].duplicated().any():
        raise ValueError(
            f"Hay IDs duplicados dentro del lote {lote}"
        )

    tablas.append(df)

# Unir las noticias de todos los lotes
datos = pd.concat(tablas, ignore_index=True)

# Ordenar cronológicamente por lote
datos = datos.sort_values(
    ["news_id", "run_id"],
    kind="stable"
).reset_index(drop=True)

# Buscar el hash anterior de cada noticia
datos["hash_anterior"] = (
    datos.groupby("news_id")["text_hash"].shift(1)
)

# Clasificar las noticias
datos["estado"] = "repetida"

datos.loc[
    datos["hash_anterior"].isna(),
    "estado"
] = "nueva"

datos.loc[
    datos["hash_anterior"].notna()
    & datos["hash_anterior"].ne(datos["text_hash"]),
    "estado"
] = "actualizada"

# Construir una única versión vigente por noticia
vigentes = (
    datos.sort_values("run_id", kind="stable")
    .drop_duplicates("news_id", keep="last")
    .copy()
)

# Conservar todas las fuentes conocidas
def unir_fuentes(valores):
    fuentes = set()

    for valor in valores.dropna():
        fuentes.update(
            f.strip()
            for f in str(valor).split(",")
            if f.strip()
        )

    return ",".join(sorted(fuentes))


fuentes = datos.groupby("news_id")["fuentes"].agg(
    unir_fuentes
)

vigentes["fuentes"] = (
    vigentes["news_id"].map(fuentes)
)

# Registrar primera aparición
primera_aparicion = (
    datos.groupby("news_id")["run_id"].min()
)

vigentes["first_seen_run_id"] = (
    vigentes["news_id"].map(primera_aparicion)
)

vigentes = vigentes.drop(columns=["hash_anterior"])

# Guardar los resultados
vigentes.to_parquet(
    SALIDA / "news_current.parquet",
    index=False
)

datos.to_parquet(
    SALIDA / "news_events.parquet",
    index=False
)

# Mostrar estadísticas
print("\n===== RESULTADO INCREMENTAL =====")

print("Lotes procesados:", len(archivos))
print("Entradas válidas:", len(datos))
print("Noticias vigentes:", len(vigentes))

print("\nClasificación:")
print(datos["estado"].value_counts().to_string())

print("\nResultados por lote:")
print(
    datos.groupby(["run_id", "estado"])
    .size()
    .to_string()
)

print("\nArchivos generados:")
print(SALIDA / "news_current.parquet")
print(SALIDA / "news_events.parquet")
