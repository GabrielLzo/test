from pyspark.sql import SparkSession
from pyspark. sql import functions as F

from funtions_main import (
    extraer_archivos_json,
    leer_archivos_json,
    procesar_estructura_json,
    leer_diccionario,
    realizar_join_mapeo,
    construir_queries,
    save_result
)

from funtions_silver import (
    obtener_ids_ejecucion,
    procesar_tablas_silver,
    crear_pivotes_silver,
    ejecutar_validacion_silver
)

from config import Config, ConfigSilver

# Configuración
spark = SparkSession.builder. appName("ValidacionSilverOroJson"). getOrCreate()
config = Config()
config_silver = ConfigSilver()

print("\n" + "="*70)
print(" PROCESO DE VALIDACIÓN SILVER VS ORO VS JSON")
print("="*70)
print(f"\nID de Ejecución: {config.ID_EJECUCION}")
print(f"Esquema ORO: {config.FUENTE_DE_DATOS['esquema_oro']}")

# =========================================================================
#  PROCESAMIENTO ORO
# =========================================================================

print("\n" + "*"*70)
print(" PROCESAMIENTO ORO")
print("*"*70)

print(" EXTRACCION DE ARCHIVOS JSON")
print("="*70)
resultado_descarga = extraer_archivos_json(spark)
print(f"JSONs extraídos: {resultado_descarga.get('exitosos', 0)}")

print("\n" + "="*70)
print(" LECTURA DE ARCHIVOS JSON")
print("="*70)
df_json = leer_archivos_json(spark)

print("\n" + "="*70)
print(" PROCESAMIENTO DE ESTRUCTURA JSON")
print("="*70)
try:
    df_campos,df_archivos_no_procesados = procesar_estructura_json(spark, df_json)
    print("Proceso de extraccion de campos exitoso")
except Exception as e:
    print("Ocurrio un error al procesar la estructura del json: ", e)
    raise

print("\n" + "="*70)
print(" LECTURA DE DICCIONARIO")
print("="*70)
try:
    df_diccionario = leer_diccionario(spark)
    print("Lectura del diccionario exitoso")
except Exception as e:
    print("Ocurrio un error al leer el diccionario: ", e)
    raise

print("\n" + "="*70)
print(" JOIN MAPEO")
print("="*70)
try:
    df_mapeado, df_sin_match = realizar_join_mapeo(spark, df_campos, df_diccionario)
    print("Join mapeo exitoso")
except Exception as e:
    print("Ocurrio un error al realizar el join mapeo: ", e)
    raise

if df_mapeado.take(1)==False:
    raise ValueError("No se mapearon registros. Verificar diccionario y claves.")

print("\n" + "="*70)
print(" CONSTRUCCION DE QUERIES")
print("="*70)
try:
    df_mapeado_queries = construir_queries(spark, df_mapeado)
    print("Construccion de queries exitoso")
except Exception as e:
    print("Ocurrio un error al construir las queries: ", e)
    raise

if "query" not in df_mapeado_queries.columns:
    raise ValueError("No se creó la columna 'query' en construir_queries")

print("\n" + "*"*70)
print("FASE ORO COMPLETADA")
print("*"*70)

# =========================================================================
#  PROCESAMIENTO SILVER
# =========================================================================
print("\n" + "*"*70)
print(" PROCESAMIENTO PLATA")
print("*"*70)

print("OBTENCIÓN DE IDS DE EJECUCIÓN ")
print("="*70)
try:
    df_mapeado_enriquecido = obtener_ids_ejecucion(
    spark, 
    df_mapeado_queries,
    config. FUENTE_DE_DATOS['esquema_oro']
    )
    print("Obtención de ids de ejecución exitoso")
except Exception as e:
    print("Ocurrio un error al ontener los id de ejecución ", e)
    raise

print("\n" + "="*70)
print("PROCESAMIENTO DE TABLAS PLATA")
print("="*70)
try:
    procesar_tablas_silver(
    spark,
    df_mapeado_enriquecido,
    config.FUENTE_DE_DATOS['esquema_oro'],
    config_silver
    )
    print("Procesamiento exitoso")
except Exception as e:
    print("Ocurrio un error al  ", e)
    raise

print("\n" + "="*70)
print("CREACION DE QUERYS PLATA")
print("="*70)
try:
    df_mapeado_silver = crear_pivotes_silver(
    spark,
    df_mapeado_enriquecido,
    config_silver.TABLAS_EXCLUIDAS_TIPO_FORMULARIO
    )
    print("Creacion de querys exitoso")
except Exception as e:
    print("Ocurrio un error al  ", e)
    raise

if "query2" not in df_mapeado_silver.columns:
    raise ValueError("No se creó la columna 'query2' en crear_pivotes_silver")

print("\n" + "*"*70)
print("FASE PLATA COMPLETADA")
print("*"*70)

print("\n" + "="*70)
print(" EJECUCION DE VALIDACIONES")
print("="*70)
try:
    config_silver_exec = ConfigSilver()
    config_silver_exec. CACHEAR_TABLAS_TEMP = False
    config_silver_exec.MAX_WORKERS_POR_GRUPO = 4
    config_silver_exec.PROGRESS_STEP = 100
except Exception as e:
    print("Error al leer la configuración para validacion: ", e)
    raise
try:
    validacion = ejecutar_validacion_silver(
    spark,
    df_mapeado_silver,
    config. ID_EJECUCION,
    config_silver_exec
    )
    if df_archivos_no_procesados.take(1):
        print("\n" + "-"*70)
        print(" ARCHIVOS NO PROCESADOS ")
        print("-"*70)
        display(df_archivos_no_procesados)
    if df_sin_match.take(1):
        print("\n" + "-"*70)
        print(" CAMPOS NO ENCONTRADOS EN EL DICCIONARIO ")
        print("-"*70)
        display(df_sin_match)
    print("\n" + "-"*70)
    print(" RESULTADO FINAL ")
    print("-"*70)
    display(validacion)
except Exception as e:
    print("Ocurrio un error al ejecutar las validaciones: ", e)
    raise
"""
print("\n" + "="*70)
print(" GUARDANDO VALIDACIONES")
print("="*70)
try:
    folder_temp = "folder_temp_silver"
    folder_save = "folder_save_silver"
    name, ruta = save_result(spark,validacion,folder_temp,folder_save)
    print("Resultado guardado correctamente")
    print(f"Nombre del archivo: {name}")
    print(f"Ruta del archivo: {ruta}")
except Exception as e:
    print("Ocurrio un error al guardar las validaciones: ", e)
    raise
"""