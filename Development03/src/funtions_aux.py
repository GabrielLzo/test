import re
import os
import shutil
from pyspark.sql.types import *
from config import Config
import pyspark.sql.functions as F
from pyspark.sql.types import StringType

config = Config()
table_pivots = {int(k): v for k, v in config.TABLE_PIVOTS.items()}

def normalize_for_schema(r, schema_resultado):
    """Normaliza los resultados"""
    out = {}
    for f in schema_resultado.fieldNames():
        out[f] = "" if r.get(f) is None else str(r.get(f))
    for k, v in r.items():
        if k not in out:
            out[k] = "" if v is None else str(v)
    return out

def comparar_valores(valor, valor_comparar):
    """Compara dos valores con lógica de equivalencia"""
    if valor is None and valor_comparar is None:
        return 'IGUAL'
    
    if valor is None or valor_comparar is None:
        no_none = valor_comparar if valor is None else valor
        try:
            num_val = float(str(no_none))
            if abs(num_val) < 1e-10:
                return 'DIFERENTE_FORMATO'
        except:
            pass
        return 'DIFERENTE'
    
    str_valor = str(valor).strip()
    str_comparar = str(valor_comparar).strip()
    
    if str_valor == str_comparar:
        return 'IGUAL'
    
    if re.search(r'\d{4}-\d{2}-\d{2}', str_valor) or re.search(r'\d{4}-\d{2}-\d{2}', str_comparar):
        return 'DIFERENTE'
    
    if str_valor.lower() in ['true', 'false'] or str_comparar.lower() in ['true', 'false']:
        return 'DIFERENTE'
    
    try:
        num = float(str_valor)
        num_comparar = float(str_comparar)
        diferencia = abs(num - num_comparar)
        
        if diferencia < 1e-10:
            return 'DIFERENTE_FORMATO' if str_valor != str_comparar else 'IGUAL'
        else:
            return 'DIFERENTE'
    except (ValueError, TypeError):
        return 'DIFERENTE'

def normalize_result_dict(row_dict, extras,schema_resultado):
    """Normaliza para la validacion"""
    base = {}
    for field in schema_resultado.fieldNames():
        if field in extras:
            val = extras.get(field)
        else:
            val = row_dict.get(field)
        base[field] = "" if val is None else str(val)
    for k, v in extras.items():
        if k not in base:
            base[k] = "" if v is None else str(v)
    return base

def limpiar_carpeta_json(carpeta_path):
    """Elimina la carpeta jsontemp y la recrea vacía"""
    try:
        if os.path.exists(carpeta_path):
            shutil.rmtree(carpeta_path)
            os.makedirs(carpeta_path, exist_ok=True)
            
        else:
            os.makedirs(carpeta_path, exist_ok=True)
   
    except Exception as e:
        print(f"Error al limpiar carpeta: {carpeta_path}")
        print(f"Error: {e}")
        raise

def convertir_int(id_concepto_valor):
    """Convierte ID concepto a entero"""
    try:
        if isinstance(id_concepto_valor, str):
            return int(id_concepto_valor.lstrip('0')) if id_concepto_valor.lstrip('0') else 0
        elif isinstance(id_concepto_valor, (int, float)):
            return int(id_concepto_valor)
        else:
            return None
    except (ValueError, TypeError):
        return None

def get_table_pivot_key(table_name, id_concepto):
    """Obtiene clave de pivote para tabla e ID concepto"""
    id_concepto_int = convertir_int(id_concepto)
    
    if id_concepto_int is not None and id_concepto_int in table_pivots:
        concepto_config = table_pivots[id_concepto_int]
        if table_name in concepto_config:
            return concepto_config[table_name]
    return None

def encontrar_arrays_con_campos(schema, path=""):
    """Encuentra arrays con estructura de campos en el schema"""
    arrays_encontrados = []
    
    if isinstance(schema, StructType):
        for field in schema.fields:
            nueva_ruta = f"{path}.{field.name}" if path else field.name
            
            if isinstance(field.dataType, ArrayType):
                elemento_tipo = field.dataType.elementType
                
                if isinstance(elemento_tipo, StructType):
                    tiene_campos = any(f.name == "Campos" for f in elemento_tipo.fields)
                    
                    if tiene_campos:
                        arrays_encontrados.append((field.name, nueva_ruta))
                    
                    arrays_encontrados.extend(
                        encontrar_arrays_con_campos(elemento_tipo, nueva_ruta)
                    )
            
            elif isinstance(field.dataType, StructType):
                arrays_encontrados.extend(
                    encontrar_arrays_con_campos(field.dataType, nueva_ruta)
                )
    
    return arrays_encontrados


def extraer_campos(spark, df_periodos, nombre_array, ids_en_datos):
    """Extrae campos de array"""
    try:
        # Buscar la clave pivote
        all_pivots = {}
        for id_concepto in ids_en_datos:
            pivot_key = get_table_pivot_key(nombre_array, id_concepto)
            if pivot_key is not None:
                all_pivots[nombre_array.lower()] = pivot_key
                break
        clave_pivote_val = all_pivots.get(nombre_array.lower())
        
        # Verificar que el array existe en el esquema
        try:
            df_periodos.select(f"Periodo.{nombre_array}").limit(1).collect()
        except Exception as e:
            return None

        df_base = df_periodos.select(
            F.col("Numero_JSON"),
            F.col("Archivo_JSON"),
            F.col("RFC"),
            F.col("Ejercicio"),
            F.col("FechaActualizacion"),
            F.col("Id_Concepto"),
            F.col("Descripcion"),
            F.col("Periodo.IdDeclaracion").alias("IdDeclaracion"),
            F.col("Periodo.IdEstatusPago").alias("IdEstatusPago_Valor"),
            F.col(f"Periodo.{nombre_array}").alias("Array_Target")
        ).filter(
            F.col("Array_Target").isNotNull() &
            (F.size(F.col("Array_Target")) > 0)
        )

        # Explode del array
        df_exploded = df_base.select(
            F.col("Numero_JSON"),
            F.col("Archivo_JSON"),
            F. col("RFC"),
            F.col("Ejercicio"),
            F.col("FechaActualizacion"),
            F.col("Id_Concepto"),
            F.col("Descripcion"),
            F.col("IdDeclaracion"),
            F. col("IdEstatusPago_Valor"),
            F. explode_outer(F.col("Array_Target")).alias("Array_Item")
        ). drop("Array_Target")

        # Buscar valor pivote
        if clave_pivote_val is not None:
            try:
                test_campos = df_exploded.select("Array_Item.Campos").limit(1).collect()
                
                if test_campos and test_campos[0][0] is not None:
                    buscar_valor_udf = F.udf(buscar_valor_en_campos, StringType())
                    df_with_valor_pivote = df_exploded.withColumn(
                        "Valor_Pivote_Source",
                        buscar_valor_udf(F.col("Array_Item.Campos"), F.lit(clave_pivote_val))
                    )
                else:
                    df_with_valor_pivote = df_exploded.withColumn(
                        "Valor_Pivote_Source",
                        F. lit(None). cast(StringType())
                    )
            except Exception as e:
                df_with_valor_pivote = df_exploded.withColumn(
                    "Valor_Pivote_Source",
                    F.lit(None).cast(StringType())
                )
        else:
            df_with_valor_pivote = df_exploded. withColumn(
                "Valor_Pivote_Source",
                F.lit(None).cast(StringType())
            )
        # Explotar Campos
        try:
            df_campos_test = df_with_valor_pivote.select("Array_Item.Campos").limit(1).collect() 
            if df_campos_test and df_campos_test[0][0] is not None:
                
                # Explotar campos
                df_with_pivot = df_with_valor_pivote.select(
                    "Numero_JSON", "Archivo_JSON", "RFC", "Ejercicio", "FechaActualizacion",
                    "Id_Concepto", "Descripcion", "IdDeclaracion", "IdEstatusPago_Valor",
                    "Valor_Pivote_Source",
                    F.explode_outer("Array_Item.Campos").alias("Campo")
                )
                
                df_temp = df_with_pivot.select(
                    F.col("Numero_JSON"),
                    F.col("Archivo_JSON"),
                    F.col("RFC"),
                    F.col("Ejercicio"),
                    F.col("FechaActualizacion"),
                    F.col("Id_Concepto"),
                    F.col("Descripcion"),
                    F.col("IdDeclaracion"),
                    F. col("IdEstatusPago_Valor"),
                    F.col("Campo.Clave").alias("Clave"),
                    F.col("Campo.Valor"). alias("Valor_Json"),
                    F.lit(nombre_array).alias("Origen_Campo"),
                    F.lit(clave_pivote_val).cast(StringType()).alias("Clave_pivote"),
                    F.col("Valor_Pivote_Source").alias("Valor_pivote")
                ).filter(F.col("Clave").isNotNull())
            else:
                df_directo_test = df_with_valor_pivote.select("Array_Item.Clave", "Array_Item.Valor").limit(1).collect()
                if df_directo_test and df_directo_test[0][0] is not None:
                    df_temp = df_with_valor_pivote.select(
                        F.col("Numero_JSON"),
                        F.col("Archivo_JSON"),
                        F.col("RFC"),
                        F.col("Ejercicio"),
                        F.col("FechaActualizacion"),
                        F.col("Id_Concepto"),
                        F.col("Descripcion"),
                        F.col("IdDeclaracion"),
                        F. col("IdEstatusPago_Valor"),
                        F.col("Array_Item.Clave").alias("Clave"),
                        F.col("Array_Item.Valor").alias("Valor_Json"),
                        F.lit(nombre_array).alias("Origen_Campo"),
                        F.lit(clave_pivote_val).cast(StringType()).alias("Clave_pivote"),
                        F.col("Valor_Pivote_Source").alias("Valor_pivote")
                    ).filter(F.col("Clave").isNotNull())
                    
                else:
                    return None
                    
        except Exception as e:
            import traceback
            traceback.print_exc()
            return None

        if df_temp.head(1):
            return df_temp
        else:
            return None

    except Exception as e:
        import traceback
        traceback.print_exc()
        return None
    

def buscar_valor_en_campos(campos_array, clave_buscada):
    """Extrae el valor de un campo dentro de un array de campos - VERSIÓN ROBUSTA"""
    try:
        if campos_array is None or clave_buscada is None:
            return None
        
        # Convertir clave buscada a string y normalizar
        clave_buscada_str = str(clave_buscada). strip()
        
        # Iterar sobre los campos
        if isinstance(campos_array, (list, tuple)):
            for campo in campos_array:
                if campo is None:
                    continue
                
                try:
                    # Los campos pueden ser Row objects o dicts
                    # Intentar acceso como Row primero
                    try:
                        clave_raw = campo. Clave
                        valor_raw = campo.Valor
                    except (AttributeError, TypeError):
                        # Si no es Row, intentar como dict
                        if isinstance(campo, dict):
                            clave_raw = campo. get('Clave')
                            valor_raw = campo.get('Valor')
                        else:
                            continue
                    
                    # Normalizar clave del campo
                    if clave_raw is not None:
                        clave_campo_str = str(clave_raw). strip()
                        
                        # Comparación simple (sin lstrip de ceros, ya que los números son importantes)
                        if clave_campo_str == clave_buscada_str:
                            if valor_raw is not None:
                                return str(valor_raw)
                            else:
                                return None
                
                except Exception:
                    continue
        
        return None
        
    except Exception as e:
        return None


def ejecutar_query(row_data, spark_session, retry_attempts=3, retry_backoff=1.0, schema_resultado=None):
    """Ejecuta una query individual"""
    query = row_data.get("query", "")
    row = row_data.get("row", {})
    extras = {}
    estado_query = ""

    # Reintentos
    last_exc = None
    for intento in range(1, retry_attempts + 1):
        try:
            df_resultado = spark_session.sql(query)
            filas = df_resultado.limit(1).collect()

            if filas:
                fila = filas[0]
                fila_dict = fila.asDict()
                campo_real = row.get("campo_real", "")
                valor_delta = fila_dict.get(campo_real) if campo_real in fila_dict else None
                id_precarga = fila_dict.get("idEjecucionPrecarga")
                id_npsi = fila_dict.get("idEjecucionNPSI")

                # Usar la función de comparación para obtener IGUAL | DIFERENTE | DIFERENTE_FORMATO
                estado_query = comparar_valores(valor_delta, row.get("Valor_Json", ""))
                extras = {
                    "Valor_Delta_Oro": "" if valor_delta is None else str(valor_delta),
                    "idEjecucionPrecarga": "" if id_precarga is None else str(id_precarga),
                    "idEjecucionNPSI": "" if id_npsi is None else str(id_npsi),
                    "Estado_Query": estado_query,
                }
            else:
                extras = {
                    "Valor_Delta_Oro": "",
                    "idEjecucionPrecarga": "" if row.get("Estado") == "ADICIONAL" else "NO_DISPONIBLE",
                    "idEjecucionNPSI": "" if row.get("Estado") == "ADICIONAL" else "NO_DISPONIBLE",
                    "Estado_Query": "NO_ENCONTRADO",
                }

            return normalize_result_dict(row, extras, schema_resultado)

        except Exception as e:
            last_exc = e
            # Mantener un log conciso de reintentos fallidos
            print(f"  Intento {intento} falló para query IdDeclaracion={row.get('IdDeclaracion')} Clave={row.get('Clave')}: {str(e)[:200]}")

            if intento < retry_attempts:
                backoff = retry_backoff * intento
                time.sleep(backoff)
            else:
                extras = {
                    "Valor_Delta_Oro": "",
                    "idEjecucionPrecarga": "ERROR",
                    "idEjecucionNPSI": "ERROR",
                    "Estado_Query": f"ERROR: {str(e)[:200]}",
                }
                return normalize_result_dict(row, extras, schema_resultado)
            
# =====================================
# FUCIONES AUX SILVER
# =====================================

def collect_samples(df, col, limit_rows=400):
    """Extrae una muestra limpia de datos válidos de una columna para su posterior análisis"""
    rows = df.select(F.col(col).cast("string").alias(col)) \
             .where(F.col(col).isNotNull() & (F.length(F.col(col)) > 0)) \
             .limit(limit_rows). collect()
    return [r[0] for r in rows if r[0]]

def infer_json_schema(spark, samples):
    """Infiere el esquema de datos JSON"""
    from pyspark.sql.types import StructType
    rdd = spark.sparkContext.parallelize(samples)
    return spark.read.json(rdd). schema

def is_complex(dt):
    """Verifica si un tipo de dato es complejo (estructurado)"""
    from pyspark.sql.types import StructType, ArrayType
    return isinstance(dt, (StructType, ArrayType))

def full_flatten(df, max_iter=50):
    """Aplana completamente un DataFrame con estructuras JSON anidadas"""
    from pyspark.sql.types import ArrayType, StructType
    working = df
    iteration = 0
    changed = True
    
    while changed and iteration < max_iter:
        changed = False
        iteration += 1
        
        for f in working.schema.fields:
            cname = f.name
            dt = f.dataType
            
            if isinstance(dt, ArrayType):
                working = working.withColumn(cname, F.explode_outer(F.col(cname)))
                changed = True
            elif isinstance(dt, StructType):
                for sub in dt.fields:
                    new_name = sub.name if sub.name not in working.columns else f"{sub.name}_{iteration}"
                    try:
                        working = working. withColumn(new_name, F.col(cname).getField(sub.name))
                    except:
                        continue
                working = working.drop(cname)
                changed = True
    return working

def normalize_json_name(name, renombrar_fila=True):
    """Normaliza nombres de columnas, especialmente el campo fila"""
    return "Fila" if renombrar_fila and name.lower() == "fila" else name

def rename_case_insensitive_to_unique(df, existing_lower, suffix_base, protected_cols, renombrar_fila=True):
    """Renombra columnas para evitar duplicados"""
    protected_lower = set(c.lower() for c in protected_cols)
    for c in df.columns:
        if c. lower() in protected_lower:
            existing_lower.add(c.lower())
            continue
        target = normalize_json_name(c, renombrar_fila)
        base_target = target
        k = 1
        while target. lower() in existing_lower:
            target = f"{base_target}_{suffix_base}_{k}"
            k += 1
        if target != c:
            df = df.withColumnRenamed(c, target)
        existing_lower.add(target. lower())
    return df

def expand_filaN_to_rows(working, parsed_col, keys_plus, fila_regex):
    """Expande campos numerados del tipo "fila1, fila2, fila3..." en filas separadas para su posterior aplanado"""
    from pyspark.sql.types import StructType
    field = next((f for f in working.schema. fields if f.name == parsed_col), None)
    if not field or not isinstance(field.dataType, StructType):
        return None
    
    fila_fields = [(sub.name, int(m.group(1))) for sub in field.dataType.fields 
                   if (m := fila_regex.match(sub. name))]
    if not fila_fields:
        return None
    
    fila_fields.sort(key=lambda x: x[1])
    dfs = []
    for name, num in fila_fields:
        item = working.select(*keys_plus, F.lit(num).alias("Fila"), 
                              F.col(f"{parsed_col}. {name}").alias("__row__"))
        item_flat = full_flatten(item)
        if "__row__" in item_flat. columns:
            if not is_complex(item_flat.schema["__row__"]. dataType):
                item_flat = item_flat.withColumnRenamed("__row__", name)
            else:
                item_flat = item_flat.drop("__row__")
        dfs.append(item_flat)
    
    out = dfs[0]
    for d in dfs[1:]:
        out = out.unionByName(d, allowMissingColumns=True)
    return out

def limpiar_valor(val):
    """Normaliza y limpia valores para comparaciones"""
    if val is None:
        return "NULL"
    s = str(val). strip()
    return "NULL" if s == "" or s. lower() in ('none', 'nan', 'null') else s

def comparar_valores_silver(valor_silver, valor_comparar):
    """Compara dos valores y categoriza el tipo de diferencia encontrada"""
    #if valor_silver == "NULL":
     #   return 'NO_ENCONTRADO'
    if valor_silver is None and valor_comparar is None:
        return 'IGUAL'
    if valor_silver is None or valor_comparar is None:
        return 'DIFERENTE'
    
    str_silver = str(valor_silver).strip()
    str_comparar = str(valor_comparar).strip()
    
    if str_silver == str_comparar:
        return 'IGUAL'
    if re.search(r'\d{4}-\d{2}-\d{2}', str_silver) or re.search(r'\d{4}-\d{2}-\d{2}', str_comparar):
        return 'DIFERENTE'
    if str_silver.lower() in ['true', 'false'] or str_comparar.lower() in ['true', 'false']:
        return 'DIFERENTE'
    
    try:
        num_silver = float(str_silver)
        num_comparar = float(str_comparar)
        return 'DIFERENTE_FORMATO' if abs(num_silver - num_comparar) < 1e-10 and str_silver != str_comparar else 'DIFERENTE' if abs(num_silver - num_comparar) >= 1e-10 else 'IGUAL'
    except:
        return 'DIFERENTE'

def cachear_tablas_temporales(spark, tareas):
    """Optimiza el rendimiento pre-cacheando tablas que serán usadas múltiples veces"""
    import re
    tablas_unicas = set()
    for tarea in tareas:
        query = tarea.get('query2', '')
        if query and query not in ["VALOR_SOLO_EN_PRECARGA", "SIN_QUERY", ""]:
            try:
                tablas_unicas.update(re.findall(r'\b(? :FROM|JOIN)\s+([a-zA-Z_][a-zA-Z0-9_]*)\b', query, re.IGNORECASE))
            except:
                pass
    
    for tabla in sorted(tablas_unicas):
        try:
            spark.table(tabla). cache(). count()
        except:
            pass
