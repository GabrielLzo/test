from datetime import date
import os
from pyspark.sql.types import *

class Config:
  ID_EJECUCION = "8ABD0823-79F5-40B9-A6E1-0DC2252C8A9C"
  
  # =====================================
  # PATHS
  # =====================================
  WORKSPACE = "/Workspace/Users/miguel.lazo@mobiik.com/Pruebas%20Caja%20Negra/ID8607_Precarga_Morales_2025/Development03"

  PATHS = {
    "diccionario": f"{WORKSPACE}/Diccionario/diccionario_precarga_silver_ACTUALIZADO.csv",
    "ruta_diccionario": f"file:{WORKSPACE}/Diccionario/diccionario_precarga_silver_ACTUALIZADO.csv",
    "carpeta_destino": f"{WORKSPACE}/jsontemp",
    "ruta_json": f"file:{WORKSPACE}/jsontemp"
  }
  # =====================================
  # FUENTE DE DATOS
  # =====================================
  FUENTE_DE_DATOS = {
    "tabla_consolidado": "consolidado_declaracionconcepto",
    "esquema_oro": "precarga_anualmorales"
  }

  # =====================================
  # TABLE_PIVOTS IDENTIFICADORES
  # =====================================  
  TABLE_PIVOTS = {
            "101": {
                "EstimuloPagoExtranjero": "0424001",
                "Estimulos": "0417001",
                "PerdidasFiscales": "0412001"
            },
            "140": {
                "EstimuloPagoExtranjero": "0424001",
                "Estimulos": "0417001",
                "PerdidasFiscales": "0412001"
            },
            "145": {
                "PerdidaFiscalDetalle": "AMC11C",
                "Estimulos": "901734"
            },
            "159": {
                "EstimuloPagoExtranjero": "0424001",
                "PerdidasFiscales": "0412001"
            },
            "167": {
                "EstimuloPagoExtranjero": "0424001",
                "Estimulos": "0417001",
                "PerdidasFiscales": "0412001"
            },
            "170": {"PerdidasFiscales": "253016"},
            "171": {"PerdidasFiscales": "253016"},
            "173": {"PerdidasFiscales": "253016"},
            "174": {"PerdidasFiscales": "253016"},
            "176": {"PerdidasFiscales": "253016"},
            "192": {
                "SIFTotales": "0710091", 
                "RDOTotales": "0720091",
                "FLETotales": "0740091",
                "CCFTotales": "0750091",
                "CPCTotales": "0730091",
                "CCFDetalle": "0750007",
                "CPCDetalle": "0730005",
                "FLEDetalle": "0740005",
                "RDODetalle": "0720007",
                "SIFDetalle": "0710007"
            }
  }

  # =====================================
  # ESTRUCTURA DEL DICCIONARIO
  # =====================================
  COLUMNAS_DICCIONARIO = {
          "format": "csv",
          "col_id": "id",
          "col_nombre_tabla": "nombre_tabla",
          "col_nombre_columna": "nombre_campo",
          "col_base_datos": "Nombre base de datos",
          "col_tabla_origen": "tabla_origen",
          "col_campo_origen": "campo_origen",
          "col_estado": "estado",
          "col_tipo_dato": "tipo_dato",
          "col_atributo": "atributo",
          "col_campo_anidado": "campo_anidado",

      }
  # =====================================
  # COLUMNAS
  # =====================================
  COLUMNAS_BASICAS = [
    "Numero_JSON", "Archivo_JSON", "RFC", "Ejercicio", "IdDeclaracion",
    "Id_Concepto", "Clave", "Campo_SAT", "Valor_Json",
    "Nombre_Tabla", "Nombre_Columna", "Estado"
  ]

  COLS_BASE = [
        "Numero_JSON", "Archivo_JSON", "RFC", "Ejercicio", "IdDeclaracion",
        "Id_Concepto", "Clave", "Campo_SAT", "Valor_Json",
        "Nombre_Tabla", "Nombre_Columna", "Estado",
        "tabla_real", "campo_real", "tabla_completa", "query"
  ]
  
  # =====================================
  # ESQUEMAS
  # =====================================
  SCHEMA_RESULTADO = StructType([
        StructField("Numero_JSON", StringType(), True),
        StructField("Archivo_JSON", StringType(), True),
        StructField("RFC", StringType(), True),
        StructField("Ejercicio", StringType(), True),
        StructField("IdDeclaracion", StringType(), True),
        StructField("Id_Concepto", StringType(), True),
        StructField("Clave", StringType(), True),
        StructField("Campo_SAT", StringType(), True),
        StructField("Valor_Json", StringType(), True),
        StructField("Nombre_Tabla", StringType(), True),
        StructField("Nombre_Columna", StringType(), True),
        StructField("Estado", StringType(), True),
        StructField("tabla_real", StringType(), True),
        StructField("campo_real", StringType(), True),
        StructField("tabla_completa", StringType(), True),
        StructField("query", StringType(), True),
        StructField("Valor_Delta_Oro", StringType(), True),
        StructField("idEjecucionPrecarga", StringType(), True),
        StructField("idEjecucionNPSI", StringType(), True),
        StructField("Estado_Query", StringType(), True)
  ])

  SCHEMA_REPORTE = StructType([
        StructField("Numero_JSON", IntegerType(), True),
        StructField("Archivo_JSON", StringType(), True),
        StructField("RFC", StringType(), True),
        StructField("Estado", StringType(), True),
        StructField("Razon", StringType(), True),
        StructField("Mensaje", StringType(), True)
    ])
  
  # =====================================
  # NOMBRES DE TABLAS DE RESULTADOS
  # =====================================

  TABLA_RESULTADOS =  {
    "folder_temp_oro": "/mnt/precargas-test-results/ID8607_AM_Precarga_JSON_ORO/csv_temp",
    "folder_save_oro": "/mnt/precargas-test-results/ID8607_AM_Precarga_JSON_ORO/",
    "folder_temp_silver": "/mnt/precargas-test-results/ID8607_AM_Precarga_SILVER_ORO_JSON/csv_temp",
    "folder_save_silver": "/mnt/precargas-test-results/ID8607_AM_Precarga_SILVER_ORO_JSON/",
  }

  # =====================================
  # STORAGE
  # =====================================
  AZURE_STORAGE = {
    "storage_account": "eu2pamuatsta001",
    "container_name": "uathomologacion",
    "sas_token": "sv=2023-01-03&ss=btqf&srt=sco&st=2024-10-04T16%3A46%3A28Z&se=2034-10-05T16%3A46%3A00Z&sp=rwdflacup&sig=E78U5OzLrkX9hGydt3VPjJWC49ceKfsp9DECOonqGPg%3D"
  }
# =====================================
# CONFIGURACIÓN SILVER
# =====================================
import re

class ConfigSilver:
    # =====================================
    # PARÁMETROS DE APLANADO JSON
    # =====================================
    SCHEMA_SAMPLE = 400
    MAX_FLATTEN_ITER = 50
    RENOMBRAR_FILA = True
    ELIMINAR_ID_NORM_FINAL = False

    # =====================================
    # LLAVES PARA JOIN
    # =====================================
    JOIN_KEYS = ["rfcDeclarante", "numeroOperacion", "id_ejecucion"]
    
    # ==========================================
    # TABLAS EXCLUIDAS DE FILTRO tipoFormulario
    # ==========================================   
    TABLAS_EXCLUIDAS_TIPO_FORMULARIO = [
        'silver_amdeterimpacreditable', 'silver_satcontribuyentedecla',
        'dd_dec_depmrge1_001', 'dd_dec_dpdeifp1_a', 'dd_dec_rglpmee1',
        'dd_dec_dtepmes1_d', 'dd_dec_dpfeadi1', 'dd_dec_depmrog1_002',
        'dd_dec_dpfeace1_a'
    ]
    # =====================================
    # EJECUCIÓN PARALELA
    # =====================================    

    MAX_WORKERS_POR_GRUPO = 8
    PROGRESS_STEP = 100
    CACHEAR_TABLAS_TEMP = True
    
    FILA_N_REGEX = re.compile(r"^fila(\d+)$", re.IGNORECASE)