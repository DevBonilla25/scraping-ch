import requests
import pandas as pd
import psycopg2
from psycopg2 import sql, extras
from datetime import datetime
import os
import ibm_boto3
from ibm_botocore.client import Config

# Configuración de la base de datos PostgreSQL
DB_HOST = os.getenv('DB_HOST', 'd29570ba-3bb2-43fb-b331-723ad28c0b1d.2adb0220806343e3ae11df79c89b377f.databases.appdomain.cloud')
DB_PORT = os.getenv('DB_PORT', '30751')
DB_NAME = os.getenv('DB_NAME', 'ibmclouddb')
DB_USER = os.getenv('DB_USER', 'ibm_cloud_7c554210_3e0c_4caf_8564_920105f30e77')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'pg3R1f6p9kTOyDX6Izq53Th3eP5n9wIY')

API_URL = "https://data.cityofchicago.org/resource/85ca-t3if.json"
TABLE_NAME = 'chicago_crashes_traffic'

# Tipos específicos por columna en la tabla de traffic crashes
SQL_TYPES = {
    """ Esta es la configuración de los tipos de datos de las columnas de la tabla de Chicago Crashes Traffic. """

    'crash_record_id': 'TEXT',
    'crash_date': 'TIMESTAMP',
    'posted_speed_limit': 'INTEGER',
    'traffic_control_device': 'TEXT',
    'device_condition': 'TEXT',
    'weather_condition': 'TEXT',
    'lighting_condition': 'TEXT',
    'first_crash_type': 'TEXT',
    'trafficway_type': 'TEXT',
    'alignment': 'TEXT',
    'roadway_surface_cond': 'TEXT',
    'road_defect': 'TEXT',
    'crash_type': 'TEXT',
    'intersection_related_i': 'TEXT',
    'hit_and_run_i': 'TEXT',
    'damage': 'TEXT',
    'prim_contributory_cause': 'TEXT',
    'sec_contributory_cause': 'TEXT',
    'street_no': 'INTEGER',
    'street_direction': 'TEXT',
    'street_name': 'TEXT',
    'beat_of_occurrence': 'TEXT',
    'photos_taken_i': 'TEXT',
    'statements_taken_i': 'TEXT',
    'dooring_i': 'TEXT',
    'work_zone_i': 'TEXT',
    'work_zone_type': 'TEXT',
    'workers_present_i': 'TEXT',
    'num_units': 'INTEGER',
    'most_severe_injury': 'TEXT',
    'injuries_total': 'INTEGER',
    'injuries_fatal': 'INTEGER',
    'injuries_incapacitating': 'INTEGER',
    'injuries_non_incapacitating': 'INTEGER',
    'injuries_reported_not_evident': 'INTEGER',
    'injuries_no_indication': 'INTEGER',
    'injuries_unknown': 'INTEGER',
    'crash_hour': 'INTEGER',
    'crash_day_of_week': 'INTEGER',
    'crash_month': 'INTEGER',
    'latitude': 'DOUBLE PRECISION',
    'longitude': 'DOUBLE PRECISION',
    'location': 'TEXT',
    'report_type': 'TEXT',
    'date_police_notified': 'TIMESTAMP'
}

def obtener_conexion_db():
    """Establece conexión con la base de datos PostgreSQL."""
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        return conn
    except Exception as e:
        print(f"Error al conectar con la base de datos: {e}")
        return None

def insert_all_to_database(df, table_name):
    """
    Inserta todos los registros del DataFrame en la base de datos, sin filtrar duplicados.
    Crea la tabla si no existe.
    """
    if df.empty:
        print("No hay datos para insertar.")
        return 0

    conn = obtener_conexion_db()
    if not conn:
        print("No se pudo establecer conexión con la base de datos.")
        return 0


    df.columns = df.columns.str.lower()
    columns = df.columns.tolist()

    fields_list = []
    for col in columns:
        col_type = SQL_TYPES.get(col, 'TEXT')
        fields_list.append(sql.SQL(f"{col} {col_type}"))

    create_table_query = sql.SQL(
        "CREATE TABLE IF NOT EXISTS {table} ({fields})"
    ).format(
        table=sql.Identifier(table_name),
        fields=sql.SQL(', ').join(fields_list)
    )

    inserted_count = 0
    try:
        with conn.cursor() as cur:
            cur.execute(create_table_query)
            conn.commit()

            insert_query = sql.SQL(
                """
                INSERT INTO {table} ({fields}) VALUES ({placeholders})
                """
            ).format(
                table=sql.Identifier(table_name),
                fields=sql.SQL(', ').join(map(sql.Identifier, columns)),
                placeholders=sql.SQL(', ').join(sql.Placeholder() * len(columns))
            )

            values = [
                tuple(None if pd.isna(val) else val for val in row)
                for row in df.itertuples(index=False, name=None)
            ]

            extras.execute_batch(cur, insert_query, values)
            inserted_count = len(values)
            conn.commit()

        print(f"Se insertaron {inserted_count} registros en la base de datos.")
        return inserted_count

    except Exception as e:
        print(f"Error al insertar en la base de datos: {e}")
        conn.rollback()
        return 0
    finally:
        conn.close()

def insert_db_full_crashes(path):
    ruta_txt = os.path.join(path, "csv_path_traffic.txt")
    with open(ruta_txt, "r", encoding="utf-8") as f:
        primera_linea = f.readline().strip()
        # Cargar el CSV en un DataFrame
        df = pd.read_csv(primera_linea, sep=';')
        insert_all_to_database(df, "chicago_crashes_traffic")


def obtener_datos():
    """
    Consulta la API de accidentes y transforma la respuesta JSON en un DataFrame.
    Usa el mismo enfoque que funciona en local: crear DataFrame directamente desde JSON.
    """
    try:
        response = requests.get(API_URL)
        response.raise_for_status()
        data = response.json()
    except Exception as e:
        print(f"Error al obtener datos de la API: {e}")
        return pd.DataFrame()
    
    # Crear DataFrame directamente desde la respuesta JSON
    # Esto es más eficiente y maneja automáticamente los tipos de datos
    df = pd.DataFrame(data)
    
    # Filtrar solo registros con crash_record_id
    if not df.empty and 'crash_record_id' in df.columns:
        df = df[df['crash_record_id'].notna()]
    
    # Convertir columnas a minúsculas
    df.columns = df.columns.str.lower()
    
    return df



def guardar_csv(df, path, fecha):
    """
    Guarda el DataFrame como un archivo .csv con nombre basado en la fecha actual.
        Crea el directorio si no existe
        Genera el nombre del archivo con prefijo chicago_reportCrash_traffic
        Exporta como CSV usando separador ; y codificación UTF-8 (Sirve para separalos por columnas el ;)
        Asi mantenemos una copia histórica local de los datos procesados. """
    try:
        os.makedirs(path, exist_ok=True)
        nombre = f"chicago_reportCrash_traffic_{fecha.strftime('%Y_%m_%d')}.csv"
        ruta = os.path.join(path, nombre)
        df.to_csv(ruta, index=False, sep=';', encoding='utf-8')
        print(f"Datos exportados a {ruta}")
        return ruta
    except Exception as e:
        print(f"Error al guardar CSV: {e}")
        return None


def subir_a_cos(ruta_archivo, bucket, nombre_objeto, apikey, resource_instance_id, endpoint):
    try:
        print("Intentando subir a COS...")
        cos = ibm_boto3.client("s3",
            ibm_api_key_id=apikey,
            ibm_service_instance_id=resource_instance_id,
            config=Config(signature_version="oauth"),
            endpoint_url=endpoint
        )
        with open(ruta_archivo, "rb") as archivo:
            cos.upload_fileobj(archivo, bucket, nombre_objeto)
        print(f"Archivo subido a COS: {nombre_objeto}")
    except Exception as e:
        print(f"Problemas al subir a COS: {e}")


def main(path: str):
    """Ejecuta el flujo principal: descarga de datos, inserción en la base, y guardado como CSV si hay nuevos registros.
    Consulta a la API de accidentes de tráfico
    Si hay datos, inserta en la base
    Si se insertan nuevos, exporta a CSV
    Sirve para ejecutar el proceso completo de ETL (extracción, transformación, carga).."""

    print("Obteniendo todos los datos disponibles de la API...")
    df = obtener_datos()
    if not df.empty:
        # Insertar datos en la base de datos
        inserted_count = insert_all_to_database(df, TABLE_NAME)
        
        # Guardar CSV solo si se insertaron datos
        if inserted_count > 0:
            ruta_csv = guardar_csv(df, path, datetime.now())
            if ruta_csv:
                ruta_txt = os.path.join(path, 'csv_path_traffic.txt')
                with open(ruta_txt, 'w') as f:
                    f.write(ruta_csv + '\n')
                    print("Ruta de csv guardado en text.")
                # === Parámetros de acceso a IBM COS (rellena con tus datos) ===
                BUCKET = "bucket-rel8ed"
                NOMBRE_OBJETO = os.path.basename(ruta_csv)
                APIKEY = "xv7bbYwNNuBMWqunqtY8hnq0xoKc7ENwb4HN7hkYLvyJ"
                RESOURCE_INSTANCE_ID = "crn:v1:bluemix:public:cloud-object-storage:global:a/a0d311a778b1491bbc7dab0f8108ec44:9510a7ed-4816-41c7-b7a2-7d63a9f6113f::"
                # === EL endpoint es el que se usa para subir a COS y debe ser PUBLICO ===
                ENDPOINT = "https://s3.us-south.cloud-object-storage.appdomain.cloud"
                # ============================================================
                subir_a_cos(ruta_csv, BUCKET, NOMBRE_OBJETO, APIKEY, RESOURCE_INSTANCE_ID, ENDPOINT)
            else:
                print("No se pudo guardar el archivo CSV.")
        else:
            print("No se insertaron nuevos registros en la base de datos.")
    else:
        print("No se encontraron datos en la API o ocurrió un error.")

# Punto de entrada si se ejecuta como script
if __name__ == "__main__":
    # Usar la ruta actual del proyecto
    current_dir = os.path.dirname(os.path.abspath(__file__))
    storage_path = os.path.join(current_dir, "storage")
    main(path=storage_path)
    
    # Alternativa: insertar desde CSV existente
    # insert_db_full_crashes("/home/data/chicago/traffic")
