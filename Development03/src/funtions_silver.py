import time
from typing import List, Dict
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window
from threading import Thread
import warnings
from collections import Counter
import time
from config import Config, ConfigSilver
import funtions_aux

warnings.filterwarnings('ignore')


def obtener_ids_ejecucion(spark: SparkSession, df_mapeado: DataFrame, base_datos: str) -> DataFrame:
    """
    CELDA 5.1 - PARTE 1: Obtiene idEjecucionPrecarga e idEjecucionNPSI de tablas ORO
    """
    df_originales_unique = df_mapeado.filter(F.col("Estado") == "ORIGINAL") \
                                      .select("Nombre_Tabla", "IdDeclaracion") \
                                      .distinct()
    consultas_totales = df_originales_unique.count()
    if consultas_totales == 0:
        print("  ADVERTENCIA: No hay registros ORIGINALES para consultar")
        return df_mapeado
    
    datos_lookup = []
    exitosos = 0
    
    for idx, row in enumerate(df_originales_unique.collect(), 1):
        nombre_tabla = row["Nombre_Tabla"]
        id_declaracion = row["IdDeclaracion"]
        tabla_completa = f"{base_datos}.{nombre_tabla}"
        
        try:
            query = f"""
                SELECT 
                    idEjecucionPrecarga,
                    idEjecucionNPSI
                FROM {tabla_completa}
                WHERE IdentificadorDeclaracion = '{id_declaracion}'
                LIMIT 1
            """
            
            resultado = spark.sql(query).first()
            
            if resultado:
                datos_lookup.append({
                    'IdDeclaracion': id_declaracion,
                    'idEjecucionPrecarga': resultado['idEjecucionPrecarga'],
                    'idEjecucionNPSI': resultado['idEjecucionNPSI']
                })
                exitosos += 1
                if idx % 10 == 0 or idx == consultas_totales:
                    print(f"    Procesados: {idx}/{consultas_totales} ({exitosos} exitosos)", end='\r')
            else:
                datos_lookup.append({
                    'IdDeclaracion': id_declaracion,
                    'idEjecucionPrecarga': None,
                    'idEjecucionNPSI': None
                })
                
        except Exception as e:
            print(f"\n      Error en {tabla_completa}: {str(e)[:80]}")
            datos_lookup.append({
                'IdDeclaracion': id_declaracion,
                'idEjecucionPrecarga': None,
                'idEjecucionNPSI': None
            })
    
    if datos_lookup:
        df_lookup = spark.createDataFrame(datos_lookup).dropDuplicates(["IdDeclaracion"])
        
        df_mapeado_enriquecido = df_mapeado.join(
            df_lookup,
            on="IdDeclaracion",
            how="left"
        )
        
        return df_mapeado_enriquecido
    else:
        print("    ADVERTENCIA: No se obtuvieron IDs de ejecución")
        return df_mapeado

def procesar_tablas_silver(
    spark: SparkSession, 
    df_mapeado: DataFrame, 
    base_datos: str,
    config: ConfigSilver = None
) -> Dict[str, any]:
    """
    CELDA 5.1 - PARTE 2: Procesa tablas Silver y crea vistas temporales
    """
    if config is None:
        config = ConfigSilver()
    
    cols_requeridas = ["Tabla_Origen", "campo_anidado", "idEjecucionNPSI", "RFC", "numeroOperacion"]
    cols_faltantes = [c for c in cols_requeridas if c not in df_mapeado.columns]
    
    if cols_faltantes:
        print(f"    ERROR: Faltan columnas requeridas: {cols_faltantes}")
        return {"tablas_procesadas": [], "resumen": [], "error": f"Faltan columnas: {cols_faltantes}"}
    
    mapping = (
        df_mapeado.select(
            F.col("Tabla_Origen").alias("tabla"),
            F.col("campo_anidado").alias("json_col"),
            F.lower(F.trim(F.col("idEjecucionNPSI"))).alias("id_norm"),
            F.col("RFC").alias("rfc_filter"),
            F.col("numeroOperacion").alias("op_filter")
        )
        .where(
            F.col("tabla").isNotNull() &
            F.col("json_col").isNotNull() &
            F.col("id_norm").isNotNull() &
            F.col("rfc_filter").isNotNull() &
            F.col("op_filter").isNotNull()
        )
        .dropDuplicates()
    )
    
    if mapping.rdd.isEmpty():
        return {"tablas_procesadas": [], "resumen": []}
    
    tablas = (
        mapping.groupBy("tabla")
               .agg(
                   F.collect_set("json_col").alias("json_cols"),
                   F.collect_set("id_norm").alias("ids_norm"),
                   F.collect_set("rfc_filter").alias("rfcs_filter"),
                   F.collect_set("op_filter").alias("ops_filter")
               )
               .collect()
    )
    
    resumen = []
    vistas_creadas = []
    
    for idx, row in enumerate(tablas, 1):
        tabla = row["tabla"]
        json_cols: List[str] = [c for c in row["json_cols"] if c]
        ids_norm: List[str] = [i for i in row["ids_norm"] if i]
        rfcs_filter = [r for r in row["rfcs_filter"] if r]
        ops_filter = [o for o in row["ops_filter"] if o]
        
        full_table = f"{base_datos}.{tabla}"
        
        if not spark.catalog.tableExists(full_table):
            resumen.append((tabla, 0, "tabla_inexistente"))
            continue
        
        try:
            df_base = spark.table(full_table)
        except Exception as e:
            resumen.append((tabla, 0, "error_lectura"))
            continue
        
        missing = [k for k in config.JOIN_KEYS if k not in df_base.columns]
        if missing:
            resumen.append((tabla, 0, "faltan_llaves"))
            continue
        
        df_base = df_base.withColumn("__id_norm__", F.lower(F.trim(F.col("id_ejecucion"))))
        df_filtered = df_base.where(
            F.col("__id_norm__").isin(ids_norm) &
            F.col("rfcDeclarante").isin(rfcs_filter) &
            F.col("numeroOperacion").isin(ops_filter)
        )
        
        df_final = df_filtered
        existing_lower = {c.lower() for c in df_final.columns}
        expansion_idx = 0
        added_cols_order: List[str] = []
        keys_plus = config.JOIN_KEYS + ["__id_norm__"]
        
        for jc in json_cols:
            expansion_idx += 1
            if jc not in df_filtered.columns:
                continue
            
            parsed_col = f"__parsed__{jc}"
            src_small = df_filtered.select(*keys_plus, jc)
            field = next((f for f in df_filtered.schema.fields if f.name == jc), None)
            
            if field and funtions_aux.is_complex(field.dataType):
                working = src_small.withColumn(parsed_col, F.col(jc))
            else:
                samples = funtions_aux.collect_samples(src_small, jc, config.SCHEMA_SAMPLE)
                if not samples:
                    continue
                schema = funtions_aux.infer_json_schema(spark, samples)
                working = src_small.withColumn(
                    parsed_col, 
                    F.from_json(F.col(jc).cast("string"), schema)
                )
            
            filaN_df = funtions_aux.expand_filaN_to_rows(working, parsed_col, keys_plus, config.FILA_N_REGEX)
            if filaN_df is not None:
                flattened = filaN_df
            else:
                flattened = funtions_aux.full_flatten(
                    working.drop(jc).withColumnRenamed(parsed_col, parsed_col),
                    config.MAX_FLATTEN_ITER
                )
                if parsed_col in flattened.columns:
                    if not funtions_aux.is_complex(flattened.schema[parsed_col].dataType):
                        flattened = flattened.withColumnRenamed(parsed_col, jc)
                    else:
                        flattened = flattened.drop(parsed_col)
            
            flattened = funtions_aux.rename_case_insensitive_to_unique(
                flattened,
                existing_lower,
                suffix_base=str(expansion_idx),
                protected_cols=keys_plus,
                renombrar_fila=config.RENOMBRAR_FILA
            )
            
            new_cols = [c for c in flattened.columns if c not in keys_plus]
            added_cols_order.extend(new_cols)
            df_final = df_final.join(flattened, on=keys_plus, how="left")
        
        base_cols = [c for c in df_filtered.columns if c not in json_cols]
        ordered_cols: List[str] = []
        
        for c in base_cols + config.JOIN_KEYS + ["__id_norm__"]:
            if c in df_final.columns and c not in ordered_cols:
                ordered_cols.append(c)
        
        for c in added_cols_order:
            if c in df_final.columns and c not in ordered_cols:
                ordered_cols.append(c)
        
        df_final = df_final.select(*ordered_cols)
        
        if config.ELIMINAR_ID_NORM_FINAL and "__id_norm__" in df_final.columns:
            df_final = df_final.drop("__id_norm__")
        
        try:
            df_final.createOrReplaceTempView(tabla)
            count_rows = df_final.count()
            resumen.append((tabla, count_rows, "ok"))
            vistas_creadas.append(tabla)
        except Exception as e:
            resumen.append((tabla, 0, "error_vista"))
    
    return {
        "tablas_procesadas": vistas_creadas,
        "resumen": resumen
    }

def crear_pivotes_silver(
    spark: SparkSession, 
    df_mapeado: DataFrame,
    tablas_excluidas: List[str] = None
) -> DataFrame:
    """
    CELDA 5.2: Crea pivotes para Silver y construye queries dinámicas
    """
    if tablas_excluidas is None:
        tablas_excluidas = ConfigSilver.TABLAS_EXCLUIDAS_TIPO_FORMULARIO
    
    if "Clave_pivote" not in df_mapeado.columns:
        df_mapeado = df_mapeado.withColumn("Clave_pivote_S", F.lit(None).cast(StringType()))
    else:
        lookup_df = df_mapeado.select("Clave", "Campo_origen").distinct()
        df_mapeado = df_mapeado.alias("original").join(
            lookup_df.alias("lookup"),
            F.col("original.Clave_pivote") == F.col("lookup.Clave"),
            "left"
        ).select(
            F.col("original.*"),
            F.col("lookup.Campo_origen").alias("Clave_pivote_S")
        )

    tablas_a_validar = df_mapeado.select("Tabla_Origen").distinct().filter(
        (F.col("Tabla_Origen").isNotNull()) & 
        (F.col("Tabla_Origen") != "NULL") &
        (F.col("Tabla_Origen") != "")
    ).collect()
    
    columnas_por_tabla: Dict[str, set] = {}
    for row in tablas_a_validar:
        tabla = row.Tabla_Origen
        try:
            tabla_temp = spark.table(tabla)
            columnas_por_tabla[tabla] = set(tabla_temp.columns)
        except Exception:
            columnas_por_tabla[tabla] = set()
 
    def validar_columna_existe(tabla_origen, clave_pivote_s):
        if not clave_pivote_s or clave_pivote_s.lower() == 'null':
            return None
        if not tabla_origen or tabla_origen == 'NULL':
            return None
        if tabla_origen in columnas_por_tabla and clave_pivote_s in columnas_por_tabla[tabla_origen]:
            return clave_pivote_s
        return None
    
    validar_udf = F.udf(validar_columna_existe, StringType())
    df_mapeado = df_mapeado.withColumn(
        "Clave_pivote_S",
        validar_udf(F.col("Tabla_Origen"), F.col("Clave_pivote_S"))
    )
    
    df_mapeado = df_mapeado.withColumn(
        "query2",
        F.when(
            (F.col("Tabla_Origen").isNull()) | 
            (F.col("Tabla_Origen") == "NULL") |
            (F.col("Campo_origen").isNull()) |
            (F.col("Campo_origen") == "NULL"),
            F.lit("VALOR_SOLO_EN_PRECARGA")
        ).otherwise(
            F.concat_ws("",
                F.lit("SELECT "), F.col("Campo_origen"), 
                F.lit(" FROM "), F.col("Tabla_Origen"), 
                F.lit(" WHERE rfcDeclarante = '"), F.col("RFC"), F.lit("' "),
                F.lit("AND numeroOperacion = '"), F.col("numeroOperacion"), F.lit("' "),
                F.lit("AND id_ejecucion = '"), F.col("idEjecucionNPSI"), F.lit("' "),
                F.when(
                    ~F.col("Tabla_Origen").isin(tablas_excluidas),
                    F.concat_ws("",
                        F.lit("AND tipoFormulario = '"), F.col("Id_Concepto"), F.lit("' ")
                    )
                ).otherwise(F.lit("")),
                F.when(
                    (F.col("Valor_pivote").isNotNull()) & 
                    (F.col("Valor_pivote") != "null") &
                    (F.col("Clave_pivote_S").isNotNull()) &
                    (F.col("Clave_pivote_S") != "null"),
                    F.concat_ws("",
                        F.lit("AND "), F.col("Clave_pivote_S"), 
                        F.lit(" = '"), F.col("Valor_pivote"), F.lit("'")
                    )
                ).otherwise(F.lit(""))
            )
        )
    )
  
    return df_mapeado

def ejecutar_query_oro(row_data: dict, spark) -> dict:
    """Ejecuta query de ORO (query) y extrae el valor"""
    q_oro = row_data.get('query', '')
    campo_real = row_data.get('campo_real', '')
    row_id = row_data['row_id']
    
    if not q_oro or not isinstance(q_oro, str):
        return {
            'row_id': row_id,
            'valor': "QUERY_VACIO",
            'idEjecucionPrecarga': '',
            'idEjecucionNPSI': ''
        }
    
    q_clean = q_oro.strip()
    q_upper = q_clean.upper()
    keywords_invalidos = ["VALOR_SOLO_EN_PRECARGA", "NO_APLICA", "SIN_MAPEO", "PENDIENTE"]
    if any(kw in q_upper for kw in keywords_invalidos):
        return {
            'row_id': row_id,
            'valor': q_clean,
            'idEjecucionPrecarga': '',
            'idEjecucionNPSI': ''
        }
    
    if q_clean.startswith("--") or q_clean.startswith("/*"):
        return {
            'row_id': row_id,
            'valor': q_clean,
            'idEjecucionPrecarga': '',
            'idEjecucionNPSI': ''
        }
    
    if not ('SELECT' in q_upper or 'WITH' in q_upper):
        return {
            'row_id': row_id,
            'valor': q_clean,
            'idEjecucionPrecarga': '',
            'idEjecucionNPSI': ''
        }
    
    try:
        df = spark.sql(q_clean)
        rows = df.limit(1).collect()
        
        if rows and len(rows) > 0:
            row_dict = rows[0].asDict()
            
            if campo_real and campo_real in row_dict:
                valor_raw = row_dict[campo_real]
                valor = funtions_aux.limpiar_valor(valor_raw)
            else:
                valor = "NULL"
            
            return {
                'row_id': row_id,
                'valor': valor,
                'idEjecucionPrecarga': str(row_dict.get('idEjecucionPrecarga', '')),
                'idEjecucionNPSI': str(row_dict.get('idEjecucionNPSI', ''))
            }
        else:
            return {
                'row_id': row_id,
                'valor': "SIN_RESULTADOS",
                'idEjecucionPrecarga': '',
                'idEjecucionNPSI': ''
            }
    
    except Exception as e:
        error_msg = str(e)
        
        if "PARSE_SYNTAX_ERROR" in error_msg or "ParseException" in error_msg:
            valor_error = "SYNTAX_ERROR"
        elif "Table or view not found" in error_msg:
            valor_error = "TABLA_NO_EXISTE"
        elif "Column" in error_msg and "not found" in error_msg:
            valor_error = "COLUMNA_NO_EXISTE"
        else:
            valor_error = f"ERROR: {error_msg[:100]}"
        
        return {
            'row_id': row_id,
            'valor': valor_error,
            'idEjecucionPrecarga': '',
            'idEjecucionNPSI': ''
        }


def ejecutar_query_silver(row_data, spark_session):
    """Ejecuta query de Silver y extrae el valor"""
    q_silver = row_data.get('query2', '')
    campo_origen = row_data.get('Campo_origen', '')
    row_id = row_data['row_id']
    
    if not q_silver or not isinstance(q_silver, str):
        return {'row_id': row_id, 'valor': "QUERY_VACIO"}
    
    q_clean = q_silver.strip()
    q_upper = q_clean.upper()
    
    if not ('SELECT' in q_upper or 'WITH' in q_upper):
        return {'row_id': row_id, 'valor': q_clean}
    
    try:
        df = spark_session.sql(q_clean)
        rows = df.limit(1).collect()
        
        if not rows or len(rows) == 0:
            return {'row_id': row_id, 'valor': "SIN_RESULTADOS"}
        
        row_dict = rows[0].asDict()
        
        metadata_cols = {
            'idEjecucionPrecarga', 'IdDeclaracion', 'RFC', 
            'idEjecucionNPSI', 'id_ejecucion', 'numeroOperacion',
            'rfcDeclarante', 'tipoFormulario', 'anioGeneroPerdida'
        }
        
        if campo_origen and campo_origen in row_dict:
            valor_raw = row_dict[campo_origen]
            return {'row_id': row_id, 'valor': funtions_aux.limpiar_valor(valor_raw)}
        
        for col_name in df.columns:
            if col_name not in metadata_cols:
                valor_raw = row_dict[col_name]
                if valor_raw is not None:
                    return {'row_id': row_id, 'valor': funtions_aux.limpiar_valor(valor_raw)}
        
        return {'row_id': row_id, 'valor': "NULL"}
    
    except Exception as e:
        err_msg = str(e)
        
        if "PARSE_SYNTAX_ERROR" in err_msg or "ParseException" in err_msg:
            return {'row_id': row_id, 'valor': "SYNTAX_ERROR"}
        elif "Table or view not found" in err_msg:
            return {'row_id': row_id, 'valor': "TABLA_NO_EXISTE"}
        elif "Column" in err_msg and "not found" in err_msg:
            return {'row_id': row_id, 'valor': "COLUMNA_NO_EXISTE"}
        else:
            return {'row_id': row_id, 'valor': f"ERROR: {err_msg[:100]}"}

def ejecutar_validacion_silver(
    spark: SparkSession,
    df_mapeado: DataFrame,
    id_ejecucion: str = "MANUAL_EXEC",
    config: ConfigSilver = None
) -> DataFrame:
    """
    CELDA 6.1: Ejecuta validación completa Silver vs Oro vs JSON - OPTIMIZADO PARA DATABRICKS
    """
    if config is None:
        config = ConfigSilver()
    
    t0 = time.time()
    
    cols_requeridas = ["query2", "query", "Campo_origen", "campo_real", "Valor_Json", "RFC", "Ejercicio", "IdDeclaracion"]
    cols_faltantes = [c for c in cols_requeridas if c not in df_mapeado.columns]
    if cols_faltantes:
        raise ValueError(f"Faltan columnas requeridas: {cols_faltantes}")
    
    df_prep = df_mapeado.withColumn(
        "Ruta_Silver_Calc", 
        F.concat_ws(".", 
                    F.coalesce(F.col("Tabla_Origen"), F.lit("")), 
                    F.coalesce(F.col("Campo_origen"), F.lit("")))
    ).withColumn(
        "Ruta_Oro_Calc", 
        F.concat_ws(".", 
                    F.coalesce(F.col("tabla_completa"), F.lit("")), 
                    F.coalesce(F.col("campo_real"), F.lit("")))
    ).withColumn(
        "row_id", F.monotonically_increasing_id()
    )
    
    cols_drop = ["idEjecucionPrecarga", "idEjecucionNPSI"]
    for col in cols_drop:
        if col in df_prep.columns:
            df_prep = df_prep.drop(col)
    
    cols_necesarias = [
        "row_id", "Archivo_JSON", "RFC", "Ejercicio", "IdDeclaracion", 
        "Id_Concepto", "Clave", "Valor_Json", "Estado", 
        "query", "query2", "Campo_origen", "campo_real",
        "Ruta_Silver_Calc", "Ruta_Oro_Calc", "Numero_JSON"
    ]
    
    cols_finales = [c for c in cols_necesarias if c in df_prep.columns]
    tareas = [row.asDict() for row in df_prep.select(*cols_finales).collect()]
    
    if config.CACHEAR_TABLAS_TEMP:
        funtions_aux.cachear_tablas_temporales(spark, tareas)
   
    resultados_oro = {}
    resultados_silver = {}
    t_start = time.time()
    
    def ejecutar_grupo_oro():
        total_tareas = len(tareas)
        procesados = 0
        errores = 0
        cache_queries = {}
        
        tablas = []
        for tarea in tareas:
            query = tarea.get('query', '')
            if 'FROM' in query.upper():
                import re
                matches = re.findall(r'FROM\s+(\w+)', query.upper())
                tablas.extend(matches)
        
        contador_tablas = Counter(tablas)
        for tabla, frecuencia in contador_tablas.most_common(3):
            if frecuencia > 5:
                try:
                    spark.table(tabla).cache().count()
                except Exception:
                    pass
        
        BATCH_SIZE = min(getattr(config, 'BATCH_SIZE', 50), 100)
        
        for batch_start in range(0, total_tareas, BATCH_SIZE):
            batch_end = min(batch_start + BATCH_SIZE, total_tareas)
            batch_tareas = tareas[batch_start:batch_end]
            
            for idx, tarea in enumerate(batch_tareas):
                tarea_global_idx = batch_start + idx
                row_id = tarea.get('row_id', tarea_global_idx)
                query = tarea.get('query', '')
                
                try:
                    if query in cache_queries:
                        resultado_cache = cache_queries[query]
                        if resultado_cache['status'] == 'OK':
                            valor = resultado_cache['valor']
                            id_precarga = resultado_cache.get('idEjecucionPrecarga', '')
                            id_npsi = resultado_cache.get('idEjecucionNPSI', '')
                        else:
                            valor = resultado_cache['error']
                            id_precarga = 'ERROR'
                            id_npsi = 'ERROR'
                            errores += 1
                    else:
                        if query and query.strip() and query.strip() != "":
                            df_resultado = spark.sql(query)
                            resultado_collect = df_resultado.limit(1).collect()
                            
                            if resultado_collect:
                                fila_dict = resultado_collect[0].asDict()
                                campo_real = tarea.get('campo_real', '')
                                valor = funtions_aux.limpiar_valor(fila_dict.get(campo_real, 'Campo no encontrado'))
                                id_precarga = str(fila_dict.get('idEjecucionPrecarga', ''))
                                id_npsi = str(fila_dict.get('idEjecucionNPSI', ''))
                                
                                cache_queries[query] = {
                                    'status': 'OK', 
                                    'valor': valor,
                                    'idEjecucionPrecarga': id_precarga,
                                    'idEjecucionNPSI': id_npsi
                                }
                            else:
                                valor = 'Sin resultados'
                                id_precarga = 'NO_DISPONIBLE'
                                id_npsi = 'NO_DISPONIBLE'
                                errores += 1
                                cache_queries[query] = {'status': 'ERROR', 'error': valor}
                        else:
                            valor = 'Query vacía'
                            id_precarga = ''
                            id_npsi = ''
                            errores += 1
                    
                    resultados_oro[row_id] = {
                        'row_id': row_id,
                        'valor': valor,
                        'idEjecucionPrecarga': id_precarga,
                        'idEjecucionNPSI': id_npsi
                    }
                    procesados += 1
                    
                except Exception as e:
                    errores += 1
                    error_msg = f"ERROR: {str(e)[:200]}"
                    cache_queries[query] = {'status': 'ERROR', 'error': error_msg}
                    
                    resultados_oro[row_id] = {
                        'row_id': row_id,
                        'valor': error_msg,
                        'idEjecucionPrecarga': 'ERROR',
                        'idEjecucionNPSI': 'ERROR'
                    }
                    procesados += 1
                    
                    if errores <= 3:
                        print(f"\n Error ORO {errores}: {str(e)[:100]}")
            
            if procesados % getattr(config, 'PROGRESS_STEP', 10) == 0 or procesados == total_tareas:
                elapsed = time.time() - t_start
                rate = procesados / elapsed if elapsed > 0 else 0
                porcentaje = procesados / total_tareas
                restantes = total_tareas - procesados
                eta = restantes / rate if rate > 0 else float('inf')
                
                print(f"\n ORO: [{porcentaje:.0%}] {procesados:,}/{total_tareas:,} | {rate:.1f} q/s | ETA: {eta:.0f}s |  {errores:,}", end='\r')
        
        return procesados, errores

    def ejecutar_grupo_silver():
        total_tareas = len(tareas)
        procesados = 0
        errores = 0
        cache_queries = {}
        
        tablas = []
        for tarea in tareas:
            query2 = tarea.get('query2', '')
            if query2 and 'FROM' in query2.upper():
                import re
                matches = re.findall(r'FROM\s+(\w+)', query2.upper())
                tablas.extend(matches)
        
        contador_tablas = Counter(tablas)
        for tabla, frecuencia in contador_tablas.most_common(3):
            if frecuencia > 5:
                try:
                    spark.table(tabla).cache().count()
                except Exception:
                    pass
        
        BATCH_SIZE = min(getattr(config, 'BATCH_SIZE', 50), 100)
        
        for batch_start in range(0, total_tareas, BATCH_SIZE):
            batch_end = min(batch_start + BATCH_SIZE, total_tareas)
            batch_tareas = tareas[batch_start:batch_end]
            
            for idx, tarea in enumerate(batch_tareas):
                tarea_global_idx = batch_start + idx
                row_id = tarea.get('row_id', tarea_global_idx)
                query2 = tarea.get('query2', '')
                
                try:
                    if query2 in cache_queries:
                        resultado_cache = cache_queries[query2]
                        if resultado_cache['status'] == 'OK':
                            valor = resultado_cache['valor']
                        else:
                            valor = resultado_cache['error']
                            errores += 1
                    else:
                        if query2 and query2.strip() not in ["VALOR_SOLO_EN_PRECARGA", "SIN_QUERY", ""]:
                            df_resultado = spark.sql(query2)
                            resultado_collect = df_resultado.limit(1).collect()
                            
                            if resultado_collect:
                                fila_dict = resultado_collect[0].asDict()
                                campo_origen = tarea.get('Campo_origen', '')
                                valor = funtions_aux.limpiar_valor(fila_dict.get(campo_origen, 'Campo no encontrado'))
                                cache_queries[query2] = {'status': 'OK', 'valor': valor}
                            else:
                                valor = 'Sin resultados'
                                errores += 1
                                cache_queries[query2] = {'status': 'ERROR', 'error': valor}
                        else:
                            valor = 'VALOR_SOLO_EN_PRECARGA'
                    
                    resultados_silver[row_id] = {
                        'row_id': row_id,
                        'valor': valor
                    }
                    procesados += 1
                    
                except Exception as e:
                    errores += 1
                    error_msg = f"ERROR: {str(e)[:200]}"
                    cache_queries[query2] = {'status': 'ERROR', 'error': error_msg}
                    
                    resultados_silver[row_id] = {
                        'row_id': row_id,
                        'valor': error_msg
                    }
                    procesados += 1
                    
                    if errores <= 3:
                        print(f"\n Error SILVER {errores}: {str(e)[:100]}")
            
            if procesados % getattr(config, 'PROGRESS_STEP', 10) == 0 or procesados == total_tareas:
                elapsed = time.time() - t_start
                rate = procesados / elapsed if elapsed > 0 else 0
                porcentaje = procesados / total_tareas
                restantes = total_tareas - procesados
                eta = restantes / rate if rate > 0 else float('inf')
                
                print(f"\n SILVER: [{porcentaje:.0%}] {procesados:,}/{total_tareas:,} | {rate:.1f} q/s | ETA: {eta:.0f}s |  {errores:,}", end='\r')
      
        return procesados, errores

    procesados_oro_total, errores_oro_total = ejecutar_grupo_oro()
    procesados_silver_total, errores_silver_total = ejecutar_grupo_silver()
   
    schema_oro = StructType([
        StructField("row_id", LongType(), False),
        StructField("Valor_Oro_Precarga", StringType(), True),
        StructField("idEjecucionPrecarga", StringType(), True),
        StructField("idEjecucionNPSI", StringType(), True)
    ])
    
    schema_silver = StructType([
        StructField("row_id", LongType(), False),
        StructField("Valor_Silver_Descarga", StringType(), True)
    ])
    
    df_oro_res = spark.createDataFrame(
        [(r['row_id'], r['valor'], r.get('idEjecucionPrecarga', ''), r.get('idEjecucionNPSI', '')) 
         for r in resultados_oro.values()],
        schema=schema_oro
    )
    
    df_silver_res = spark.createDataFrame(
        [(r['row_id'], r['valor']) for r in resultados_silver.values()],
        schema=schema_silver
    )
    
    df_final = df_prep \
        .join(df_oro_res, on="row_id", how="left") \
        .join(df_silver_res, on="row_id", how="left")
    
    comparar_udf = F.udf(funtions_aux.comparar_valores_silver, StringType())
    
    df_resultado = df_final.select(
        F.coalesce(F.col("Numero_JSON"), F.lit("")).alias("JSON"),
        F.col("Archivo_JSON"),
        F.col("RFC"),
        F.col("Ejercicio").cast(StringType()),
        F.col("IdDeclaracion").cast(StringType()),
        F.col("Ruta_Silver_Calc").alias("Ruta_Silver_Descarga"),
        F.col("Ruta_Oro_Calc").alias("Ruta_Oro_Precarga"),
        F.concat_ws(".", F.col("Id_Concepto"), F.col("Clave")).alias("Ruta_Json_Precarga"),
        F.coalesce(F.col("Valor_Silver_Descarga"), F.lit("NULL")).alias("Valor_Silver_Descarga"),
        F.coalesce(F.col("Valor_Oro_Precarga"), F.lit("NULL")).alias("Valor_Oro_Precarga"),
        F.coalesce(F.col("Valor_Json"), F.lit("NULL")).alias("Valor_Json_Precarga"),
        comparar_udf(
            F.coalesce(F.col("Valor_Silver_Descarga"), F.lit("NULL")), 
            F.coalesce(F.col("Valor_Oro_Precarga"), F.lit("NULL"))
        ).alias("Estado_Oro"),
        comparar_udf(
            F.coalesce(F.col("Valor_Silver_Descarga"), F.lit("NULL")), 
            F.coalesce(F.col("Valor_Json"), F.lit("NULL"))
        ).alias("Estado_Json"),
        F.coalesce(F.col("Estado"), F.lit("ORIGINAL")).alias("Tipo_Registro"),
        F.coalesce(F.col("idEjecucionPrecarga"), F.lit("")).alias("idEjecucionPrecarga"),
        F.coalesce(F.col("idEjecucionNPSI"), F.lit("")).alias("idEjecucionNPSI"),
        F.lit(id_ejecucion).alias("id_ejecucion")
    )
    
    tiempo_total = time.time() - t0
    print(f"\n VALIDACIÓN COMPLETADA EN {tiempo_total:.1f}s")
    print(f" ORO: {procesados_oro_total:,} procesados, {errores_oro_total:,} errores")
    print(f" SILVER: {procesados_silver_total:,} procesados, {errores_silver_total:,} errores")
    
    return df_resultado