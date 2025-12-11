from funtions_main import (extraer_archivos_json,leer_archivos_json,procesar_estructura_json,
    leer_diccionario,realizar_join_mapeo,construir_queries,ejecutar_validacion_completa,save_result)

from pyspark.sql import SparkSession

# Crear SparkSession
spark = SparkSession.builder.appName("ValidacionJsonOro").getOrCreate()
from config import Config
config = Config()
print("\n" + "="*80)
print(" PROCESO DE VALIDACIÓN JSON VS ORO")
print("="*80)
print(f"\nID de Ejecución: {config.ID_EJECUCION}")
print(f"Esquema ORO: {config.FUENTE_DE_DATOS['esquema_oro']}")

print("\n" + "="*70)
print(" EXTRACCION DE ARCHIVOS JSON")
print("="*70)
resultado_descarga = extraer_archivos_json(spark)

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

print("\n" + "="*70)
print(" CONSTRUCCION DE QUERIES")
print("="*70)
try:
    df_mapeado1 = construir_queries(spark, df_mapeado)
    print("Construccion de queries exitoso")
except Exception as e:
    print("Ocurrio un error al construir las queries: ", e)
    raise

print("\n" + "="*70)
print(" EJECUCION DE VALIDACIONES")
print("="*70)
try:
    validacion = ejecutar_validacion_completa(spark, df_mapeado1)
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
    display(validacion["df_resultado_final"])
except Exception as e:
    print("Ocurrio un error al ejecutar las validaciones: ", e)
    raise

print("\n" + "="*70)
print(" GUARDANDO VALIDACIONES")
print("="*70)
try:
    folder_temp = "folder_temp_oro"
    folder_save = "folder_save_oro"
    name, ruta = save_result(spark,validacion["df_resultado_final"],folder_temp,folder_save)
    print("Resultado guardado correctamente")
    print(f"Nombre del archivo: {name}")
    print(f"Ruta del archivo: {ruta}")
except Exception as e:
    print("Ocurrio un error al guardar las validaciones: ", e)
    raise