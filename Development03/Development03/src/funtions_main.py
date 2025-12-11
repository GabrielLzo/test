import os
import pyspark.sql.functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window
from functools import reduce
from datetime import datetime
import time
import traceback
from pyspark.sql.functions import col, when, udf, lit
import re
from pyspark.sql.types import StringType
from config import Config
import funtions_aux

config = Config()
base_datos = config.FUENTE_DE_DATOS["esquema_oro"]


def extraer_archivos_json(spark):
    """CELDA 0: Se extraen los JSON por id_ejecucion"""
    id_ejecucion = config.ID_EJECUCION
    storage_account = config.AZURE_STORAGE["storage_account"]
    container_name = config.AZURE_STORAGE["container_name"]
    sas_token = config.AZURE_STORAGE["sas_token"]
    CARPETA_DESTINO = config.PATHS["carpeta_destino"]
    tabla_consolidado = config.FUENTE_DE_DATOS["tabla_consolidado"]

    if not id_ejecucion or str(id_ejecucion).strip() == "":
        raise Exception(" ERROR: ID_EJECUCION no configurado en config.py")

    funtions_aux.limpiar_carpeta_json(CARPETA_DESTINO)


    # Crear vista temporal
    df_consolidado = spark.table(
        f"{config.FUENTE_DE_DATOS['esquema_oro']}.{tabla_consolidado}"
    )
    df_tmp = df_consolidado.filter(F.col("id_ejecucion") == id_ejecucion).select(
        F.concat(F.col("Rfc"), F.lit("-"), F.col("Ejercicio"), F.lit(".json")).alias(
            "nombre_archivo"
        ),
        F.col("Rfc"),
        F.col("Ejercicio"),
    )
    df_tmp.createOrReplaceTempView("tmp_rfc_ejercicio_json")
    # Configurar autenticación
    spark.conf.set(
        f"fs.azure.sas.{container_name}.{storage_account}.blob.core.windows.net",
        sas_token,
    )
    # Descargar archivos
    df_archivos = spark.sql(
        "SELECT DISTINCT nombre_archivo FROM tmp_rfc_ejercicio_json"
    )
    archivos = df_archivos.collect()
    print(f"Total de archivos a descargar: {len(archivos)}")
    exitosos = 0
    fallidos = 0
    for idx, row in enumerate(archivos, 1):
        blob_name = row.nombre_archivo
        wasbs_path = f"wasbs://{container_name}@{storage_account}.blob.core.windows.net/{blob_name}"
        try:
            print(f"[{idx}/{len(archivos)}] Descargando: {blob_name}...", end=" ")
            df_content = spark.read.option("multiline", "true").text(wasbs_path)
            content_rows = df_content.collect()
            if content_rows:
                contenido = "\n".join([row.value for row in content_rows])
                ruta_local_completa = os.path.join(CARPETA_DESTINO, blob_name)
                with open(ruta_local_completa, "w", encoding="utf-8") as f:
                    f.write(contenido)
                exitosos += 1
                print(f"OK ({len(contenido)} bytes)")
            else:
                fallidos += 1
                print("ERROR: Archivo vacío")

        except Exception as e:
            print(f"\nError al descargar \n Json no encontrado {blob_name}: {e}")
            fallidos += 1
    if exitosos == 0:
        raise Exception(f"Sin archivos JSON")
    print(f"Resumen: Exitosos: {exitosos}, Fallidos: {fallidos}")
    return {"exitosos": exitosos, "fallidos": fallidos, "carpeta": CARPETA_DESTINO}


def leer_archivos_json(spark):
    """CELDA 1: Lee archivos JSON"""
    ruta_json = config.PATHS["ruta_json"]
    print(f"Ruta JSON: {ruta_json}")
    # Validar que tiene json
    archivos = [
        f for f in os.listdir(ruta_json.replace("file:", "")) if f.endswith(".json")
    ]
    if not archivos:
        raise Exception(f"Sin JSON para procesar")
    # Leer archivos
    df_json_raw = spark.read.option("multiLine", "true").json(ruta_json)
    # Validar que tiene datos
    if len(df_json_raw.limit(1).collect()) == 0:
        raise Exception("Los archivos JSON están vacíos")
    TAMAÑO_MINIMO = 200  # bytes
    sin_claves = 0
    ruta_local = ruta_json.replace("file:", "")
    for archivo in archivos:
        ruta_completa = os.path.join(ruta_local, archivo)
        tamaño = os.path.getsize(ruta_completa)

        if tamaño < TAMAÑO_MINIMO:
            sin_claves += 1
            print(f"{archivo}: {tamaño} bytes (Sin claves)")

    print(f" {(len(archivos)-sin_claves)} archivos leídos correctamente")
    return df_json_raw


def procesar_estructura_json(spark, df_json_raw):
    """CELDA 2: Procesa estructura del JSON y extrae campos"""
    # Procesamiento inicial
    df_json_con_archivo = df_json_raw.withColumn(
        "ruta_archivo_completo", F.input_file_name()
    ).withColumn(
        "Archivo_JSON",
        F.regexp_extract(F.col("ruta_archivo_completo"), r"([^/]+\.json)$", 1),
    )
    window_spec = Window.orderBy("ruta_archivo_completo")
    df_json_numerado = df_json_con_archivo.withColumn(
        "Numero_JSON", F.row_number().over(window_spec)
    )
    archivos_originales = (
        df_json_numerado.select("Numero_JSON", "Archivo_JSON", "RFC")
        .distinct()
        .collect()
    )
    # Explotar conceptos y periodos
    df_periodos = df_json_numerado.select(
        F.col("Numero_JSON"),
        F.col("Archivo_JSON"),
        F.col("RFC"),
        F.col("Ejercicio"),
        F.col("FechaActualizacion"),
        F.explode("Conceptos").alias("Concepto"),
    ).select(
        F.col("Numero_JSON"),
        F.col("Archivo_JSON"),
        F.col("RFC"),
        F.col("Ejercicio"),
        F.col("FechaActualizacion"),
        F.col("Concepto.Id").alias("Id_Concepto"),
        F.col("Concepto.Descripcion").alias("Descripcion"),
        F.explode("Concepto.Periodos").alias("Periodo"),
    )
    ids_en_datos_rows = df_periodos.select("Id_Concepto").distinct().collect()
    ids_en_datos = [row.Id_Concepto for row in ids_en_datos_rows]

    # Descubrir estructura
    periodo_schema = df_periodos.schema["Periodo"].dataType
    arrays_anidados = funtions_aux.encontrar_arrays_con_campos(periodo_schema)

    # Procesar campos directos
    df_campos_normales = (
        df_periodos.select(
            F.col("Numero_JSON"),
            F.col("Archivo_JSON"),
            F.col("RFC"),
            F.col("Ejercicio"),
            F.col("FechaActualizacion"),
            F.col("Id_Concepto"),
            F.col("Descripcion"),
            F.col("Periodo.IdDeclaracion").alias("IdDeclaracion"),
            F.col("Periodo.IdEstatusPago").alias("IdEstatusPago_Valor"),
            F.explode_outer("Periodo.Campos").alias("Campo"),
        )
        .select(
            F.col("Numero_JSON"),
            F.col("Archivo_JSON"),
            F.col("RFC"),
            F.col("Ejercicio"),
            F.col("FechaActualizacion"),
            F.col("Id_Concepto"),
            F.col("Descripcion"),
            F.col("IdDeclaracion"),
            F.col("IdEstatusPago_Valor"),
            F.col("Campo.Clave").alias("Clave"),
            F.col("Campo.Valor").alias("Valor_Json"),
            F.lit("Campos_Directos").alias("Origen_Campo"),
            F.lit(None).cast(StringType()).alias("Clave_pivote"),
            F.lit(None).cast(StringType()).alias("Valor_pivote"),
        )
        .filter(F.col("Clave").isNotNull())
    )

    # IdEstatusPago
    df_id_estatus_pago = df_periodos.select(
        F.col("Numero_JSON"),
        F.col("Archivo_JSON"),
        F.col("RFC"),
        F.col("Ejercicio"),
        F.col("FechaActualizacion"),
        F.col("Id_Concepto"),
        F.col("Descripcion"),
        F.col("Periodo.IdDeclaracion").alias("IdDeclaracion"),
        F.col("Periodo.IdEstatusPago").alias("IdEstatusPago_Valor"),
        F.lit("IdEstatusPago").alias("Clave"),
        F.col("Periodo.IdEstatusPago").alias("Valor_Json"),
        F.lit("Metadato_SAT").alias("Origen_Campo"),
        F.lit(None).cast(StringType()).alias("Clave_pivote"),
        F.lit(None).cast(StringType()).alias("Valor_pivote"),
    ).filter(F.col("IdEstatusPago_Valor").isNotNull())

    lista_dfs = [df_campos_normales, df_id_estatus_pago]

    # Procesar arrays
    for nombre_array, ruta in arrays_anidados:
        df_temp = funtions_aux.extraer_campos(spark, df_periodos, nombre_array, ids_en_datos)
        if df_temp is not None:
            lista_dfs.append(df_temp)

    # Campos simples
    campos_simples = [
        field.name
        for field in periodo_schema.fields
        if not isinstance(field.dataType, ArrayType)
    ]

    lista_df_campos_simples = []
    for campo in campos_simples:
        campo_lower = campo.lower()
        if "pago" in campo_lower or "declaracion" in campo_lower:
            continue
        df_campo = (
            df_periodos.select(
                "Numero_JSON",
                "Archivo_JSON",
                "RFC",
                "Ejercicio",
                "FechaActualizacion",
                "Id_Concepto",
                "Descripcion",
                F.col("Periodo.IdDeclaracion").alias("IdDeclaracion"),
                F.lit(campo).alias("Clave"),
                F.col(f"Periodo.{campo}").cast("string").alias("Valor_Json"),
                F.lit("CampoDirecto").alias("Origen_Campo"),
            )
            .filter(F.col(f"Periodo.{campo}").isNotNull())
            .withColumn("Clave_pivote", F.lit(None).cast(StringType()))
            .withColumn("Valor_pivote", F.lit(None).cast(StringType()))
        )

        lista_df_campos_simples.append(df_campo)
    if lista_df_campos_simples:
        df_campos_simples = reduce(
            lambda df1, df2: df1.unionByName(df2, allowMissingColumns=True),
            lista_df_campos_simples,
        )
        lista_dfs.append(df_campos_simples)
    # Consolidación final
    df_campos_json = lista_dfs[0]
    for df in lista_dfs[1:]:
        df_campos_json = df_campos_json.unionByName(df, allowMissingColumns=True)

    df_campos_json = df_campos_json.withColumn(
        "Campo_SAT",
        F.when(F.col("Clave") == "IdEstatusPago", F.col("Clave")).otherwise(
            F.concat(F.lit("SAT_"), F.col("Clave"))
        ),
    )
    # numeroOperacion
    df_numero_operacion = (
        df_campos_json.filter(F.col("Clave") == "24006")
        .select(
            "RFC",
            "Ejercicio",
            "IdDeclaracion",
            F.col("Valor_Json").alias("numeroOperacion"),
        )
        .distinct()
    )
    has_numero_operacion = len(df_numero_operacion.take(1)) > 0
    if has_numero_operacion:
        df_campos_json = df_campos_json.join(
            df_numero_operacion, ["RFC", "Ejercicio", "IdDeclaracion"], "left"
        )
    else:
        df_campos_json = df_campos_json.withColumn(
            "numeroOperacion", F.lit(None).cast(StringType())
        )
    df_campos_json = df_campos_json.select(
        "Numero_JSON",
        "Archivo_JSON",
        "RFC",
        "Ejercicio",
        "FechaActualizacion",
        "Id_Concepto",
        "Descripcion",
        "IdDeclaracion",
        "numeroOperacion",
        "Clave",
        "Campo_SAT",
        "Valor_Json",
        "Origen_Campo",
        "Clave_pivote",
        "Valor_pivote",
    )
    # Inicia validacion de archivos procesados
    archivos_procesados_rows = (
        df_campos_json.select("Numero_JSON", "Archivo_JSON").distinct().collect()
    )
    archivos_procesados_set = {
        (row.Numero_JSON, row.Archivo_JSON) for row in archivos_procesados_rows
    }
    archivos_no_procesados = []
    for archivo in archivos_originales:
        if (archivo.Numero_JSON, archivo.Archivo_JSON) not in archivos_procesados_set:
            archivos_no_procesados.append(
                {
                    "Numero_JSON": archivo.Numero_JSON,
                    "Archivo_JSON": archivo.Archivo_JSON,
                    "RFC": archivo.RFC,
                    "Estado": "NO_PROCESADO",
                    "Razon": "SIN_CAMPOS_CLAVE",
                    "Mensaje": f'JSON "{archivo.Archivo_JSON}" (RFC: {archivo.RFC}) NO FUE PROCESADO YA QUE NO TIENE CAMPOS CLAVE PARA VALIDAR.',
                }
            )

    if len(archivos_no_procesados) > 0:
        df_archivos_no_procesados = spark.createDataFrame(archivos_no_procesados)
    else:
        schema_reporte = config.SCHEMA_REPORTE
        df_archivos_no_procesados = spark.createDataFrame([], schema_reporte)
    df_archivos_no_procesados.createOrReplaceTempView("archivos_no_procesados")


    return df_campos_json, df_archivos_no_procesados


def leer_diccionario(spark):
    """CELDA 3: Lee y procesa diccionario"""
    ruta_diccionario = config.PATHS["ruta_diccionario"]
    format_dict = config.COLUMNAS_DICCIONARIO["format"]
    df_diccionario = (
        spark.read.format(format_dict)
        .option("header", "true")
        .option("inferSchema", "true")
        .load(ruta_diccionario)
    )
    col_config = config.COLUMNAS_DICCIONARIO
    df_diccionario_filtrado = df_diccionario.select(
        F.col(col_config["col_id"]).alias("Id_Diccionario"),
        F.col(col_config["col_nombre_tabla"]).alias("Nombre_Tabla"),
        F.col(col_config["col_nombre_columna"]).alias("Nombre_Columna"),
        F.col(col_config["col_tabla_origen"]).alias("Tabla_Origen"),
        F.col(col_config["col_campo_origen"]).alias("Campo_origen"),
        F.col(col_config["col_base_datos"]).alias("Nombre_Base_Datos"),
        F.col(col_config["col_estado"]).alias("Estado"),
        F.col(col_config["col_campo_anidado"]).alias("campo_anidado"),
    ).filter(F.col(col_config["col_base_datos"]) == base_datos)
    return df_diccionario_filtrado


def realizar_join_mapeo(spark, df_campos_json, df_diccionario_filtrado):
    """CELDA 4: Realiza JOIN entre JSON y diccionario"""
    # Verificaciones
    columnas_json = df_campos_json.columns
    tiene_clave_pivote = "Clave_pivote" in columnas_json
    tiene_valor_pivote = "Valor_pivote" in columnas_json
    tiene_numero_operacion = "numeroOperacion" in columnas_json
    # JOIN principal
    df_join = df_campos_json.join(
        df_diccionario_filtrado,
        (df_campos_json.Id_Concepto == df_diccionario_filtrado.Id_Diccionario)
        & (df_campos_json.Campo_SAT == df_diccionario_filtrado.Nombre_Columna),
        "left",
    )
    df_con_match = df_join.filter(F.col("Nombre_Tabla").isNotNull()).cache()
    df_sin_match = df_join.filter(F.col("Nombre_Tabla").isNull())
    # Crear DataFrame mapeado
    columnas_basicas = Config.COLUMNAS_BASICAS
    columnas_adicionales = []
    if tiene_numero_operacion:
        columnas_adicionales.append("numeroOperacion")
    if tiene_clave_pivote:
        columnas_adicionales.append("Clave_pivote")
    if tiene_valor_pivote:
        columnas_adicionales.append("Valor_pivote")
    if "Origen_Campo" in df_con_match.columns:
        columnas_adicionales.append("Origen_Campo")

    # COLUMNAS SILVER
    columnas_silver = ["Tabla_Origen", "Campo_origen", "campo_anidado"]
    for col in columnas_silver:
        if col in df_con_match.columns:
            columnas_adicionales.append(col)

    columnas_finales = columnas_basicas + columnas_adicionales
    columnas_existentes = [
        col for col in columnas_finales if col in df_con_match.columns
    ]
    df_mapeado = df_con_match.select(*columnas_existentes)
    df_con_match.unpersist()
    sin_match1 = df_sin_match.select(df_sin_match.columns[:12])
    return df_mapeado, sin_match1

 
def construir_queries(spark, df_mapeado):
    """CELDA 5: Construye queries dinámicas"""
    base_db_lit = F.lit(f"{base_datos}.")
    df_mapeado = (
        df_mapeado.withColumn(
            "tabla_real",
            F.when(
                F.col("Estado") == "ADICIONAL",
                F.split(F.col("Nombre_Tabla"), "\\.").getItem(0),
            ).otherwise(F.col("Nombre_Tabla")),
        )
        .withColumn(
            "campo_real",
            F.when(
                F.col("Estado") == "ADICIONAL",
                F.split(F.col("Nombre_Tabla"), "\\.").getItem(1),
            ).otherwise(F.col("Nombre_Columna")),
        )
        .withColumn("tabla_completa", F.concat(base_db_lit, F.col("tabla_real")))
        .withColumn(
            "condicion_pivote",
            F.when(
                F.col("Clave_pivote").isNotNull()
                & F.col("Valor_pivote").isNotNull()
                & (F.trim(F.col("Clave_pivote")) != "")
                & (F.trim(F.col("Valor_pivote")) != ""),
                F.concat_ws(
                    "",
                    F.lit(" AND SAT_"),
                    F.col("Clave_pivote"),
                    F.lit(" = '"),
                    F.col("Valor_pivote"),
                    F.lit("'"),
                ),
            ).otherwise(F.lit("")),
        )
        .withColumn(
            "query",
            F.when(
                F.col("Estado") == "ADICIONAL",
                F.concat_ws(
                    "",
                    F.lit("SELECT "),
                    F.col("campo_real"),
                    F.lit(" FROM "),
                    F.col("tabla_completa"),
                    F.lit(" WHERE RFC = '"),
                    F.col("RFC"),
                    F.lit("' AND Ejercicio = "),
                    F.col("Ejercicio"),
                    F.lit(" AND IdentificadorDeclaracion = '"),
                    F.col("IdDeclaracion"),
                    F.lit("'"),
                    F.col("condicion_pivote"),
                ),
            ).otherwise(
                F.concat_ws(
                    "",
                    F.lit("SELECT "),
                    F.col("campo_real"),
                    F.lit(", idEjecucionPrecarga, idEjecucionNPSI FROM "),
                    F.col("tabla_completa"),
                    F.lit(" WHERE RFC = '"),
                    F.col("RFC"),
                    F.lit("' AND Ejercicio = "),
                    F.col("Ejercicio"),
                    F.lit(" AND IdentificadorDeclaracion = '"),
                    F.col("IdDeclaracion"),
                    F.lit("'"),
                    F.col("condicion_pivote"),
                )
            ),
        )
        .withColumn(
            "Nombre_Columna",
            F.when(
                F.col("Estado") == "ADICIONAL", F.concat(F.lit("SAT_"), F.col("Clave"))
            ).otherwise(F.col("Nombre_Columna")),
        )
        .drop("condicion_pivote")
    )

    return df_mapeado

def ejecutar_validacion_completa(spark, df_mapeado):
    """CELDA 6: Ejecuta validación completa """
    # Configuración original
    RETRY_ATTEMPTS = 3
    RETRY_BACKOFF = 1.0
    PROGRESS_STEP = 10
    SCHEMA_RESULTADO = Config.SCHEMA_RESULTADO
    cols_base = Config.COLS_BASE
    cols_existentes = [c for c in cols_base if c in df_mapeado.columns]
 
    # Recopilar registros 
    registros = df_mapeado.select(*cols_existentes).collect()
    tareas = [
        {"query": getattr(row, "query", ""), "row": row. asDict()} for row in registros
    ]
    
    resultados = []
    errores = 0
    procesados = 0
    t_start = time.time()

    for idx, tarea in enumerate(tareas):
        try:
            # Ejecutar query individual
            res = funtions_aux.ejecutar_query(
                tarea,
                spark,
                RETRY_ATTEMPTS,
                RETRY_BACKOFF,
                SCHEMA_RESULTADO,
            )
            
            resultados.append(res)
            procesados += 1
            
            # Contar errores
            if res. get("Estado_Query", "").startswith("ERROR"):
                errores += 1
            
            # Mostrar progreso
            if procesados % PROGRESS_STEP == 0 or procesados == len(tareas):
                elapsed = time.time() - t_start
                velocidad = procesados / elapsed if elapsed > 0 else 0
                restantes = len(tareas) - procesados
                eta = restantes / velocidad if velocidad > 0 else float("inf")
                print(
                    f"   Procesados: {procesados}/{len(tareas)} ({procesados*100//len(tareas)}%) | Velocidad: {velocidad:.2f} reg/s | ETA: {eta:.0f}s | Errores: {errores}"
                )
                
        except Exception as e:
            print(f"  Error en tarea {idx}: {str(e)[:200]}")
            errores += 1
            # Agregar resultado de error
            error_result = {
                **tarea["row"],
                "Valor_Delta_Oro": "",
                "idEjecucionPrecarga": "ERROR", 
                "idEjecucionNPSI": "ERROR",
                "Estado_Query": f"ERROR: {str(e)[:200]}"
            }
            resultados.append(error_result)
            procesados += 1

    t_total = time.time() - t_start
    print(
        f"\n  Procesamiento secuencial finalizado en {t_total:.1f}s.  Procesados: {procesados}, Errores: {errores}"
    )

    # Crear DATAFRAME resultados
    resultados_normalizados = [
        funtions_aux.normalize_for_schema(r, SCHEMA_RESULTADO) for r in resultados
    ]
    df_con_valores = spark.createDataFrame(
        resultados_normalizados, schema=SCHEMA_RESULTADO
    )

    df_con_valores = df_con_valores.withColumn(
        "Ruta_Json_Id_Clave", F.concat(F.col("Id_Concepto"), F.lit(". "), F.col("Clave"))
    ). withColumn(
        "Ruta_Delta_Oro",
        F.concat(F.col("tabla_completa"), F.lit(". "), F.col("campo_real")),
    )
    
    # Resumen
    df_resultado = (
        df_con_valores. filter(
            (F.col("Estado_Query") == "IGUAL") | (F.col("Estado_Query") == "DIFERENTE") | (F.col("Estado_Query") == "DIFERENTE_FORMATO")
        )
        .select(
            F.col("Numero_JSON"). alias("JSON"),
            F.col("Archivo_JSON"),
            F.col("RFC"),
            F.col("Ejercicio"),
            F.col("IdDeclaracion"),
            F.col("Ruta_Json_Id_Clave"). alias("Ruta_Json_Precarga"),
            F.col("Ruta_Delta_Oro").alias("Ruta_Oro_Precarga"),
            F.col("Valor_Json").alias("Valor_Json_Precarga"),
            F.col("Valor_Delta_Oro").alias("Valor_Oro_Precarga"),
            F.col("Estado_Query"). alias("Estado"),
            F.col("Estado"). alias("Tipo_Registro"),
            F.col("idEjecucionPrecarga"),
            F.col("idEjecucionNPSI"),
        )
        .orderBy("JSON", "tabla_real", "Ruta_Json_Precarga")
    )

    # Aplicar UDF de comparación 
    comparar_udf = udf(funtions_aux.comparar_valores, StringType())
    columnas_originales = df_resultado.columns
    df_resultado_final = df_resultado.withColumn(
        "Estado",
        when(col("Estado") == "IGUAL", lit("IGUAL")). otherwise(
            comparar_udf(col("Valor_Json_Precarga"), col("Valor_Oro_Precarga"))
        ),
    )

    # Reordenar columnas
    df_resultado_final = df_resultado_final.select(columnas_originales)

    no_encontrados = df_con_valores.filter(F.col("Estado_Query") == "NO_ENCONTRADO")

    return {
        "df_resultado_final": df_resultado_final,
        "df_no_encontrados": no_encontrados,
        "df_con_valores": df_con_valores,
        "errores": errores,
        "total_procesados": len(resultados),
    }

def save_result(spark,result,folder_temp,folder_save):
    """Se reciben parametros para guardar csv en storage"""
    from datetime import datetime
    from pyspark.dbutils import DBUtils
    dbutils = DBUtils(spark)
    # Se lee DF
    df = result
    df.coalesce(1).write.mode("overwrite").option("header", "true").csv(config.TABLA_RESULTADOS[folder_temp])
    name_csv = str(config.ID_EJECUCION)+"_"+str(datetime.now().strftime("%d-%m-%Y_%H-%M-%S"))+".csv"
    # Encontrar y mover el único part-file
    filename = [
        f.name
        for f in dbutils.fs.ls(config.TABLA_RESULTADOS[folder_temp])if f.name.startswith("part-")][0]
    dbutils.fs.mv(
        f"{config.TABLA_RESULTADOS[folder_temp]}/{filename}",
        config.TABLA_RESULTADOS[folder_save]+"/"+name_csv,
    )  
    # Eliminar la carpeta temporal
    dbutils.fs.rm(config.TABLA_RESULTADOS[folder_temp], recurse=True)
    return name_csv,config.TABLA_RESULTADOS[folder_save]
