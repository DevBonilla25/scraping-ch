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

API_URL = "https://data.cityofchicago.org/resource/u6pd-qa9d.json"
TABLE_NAME = 'chicago_crashes_people'

# Definición de tipos SQL para las columnas
SQL_TYPES = {
    'crash_date': 'TIMESTAMP',
    'age': 'INTEGER',
    'person_num': 'INTEGER',
    'ems_response_time': 'INTEGER',
    'ems_transport_time': 'INTEGER',
    'ems_hospital_time': 'INTEGER',
    'bac_result_value': 'DECIMAL(5,3)',
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
    Inserta todos los registros del DataFrame en la base de datos.
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

def obtener_datos():
    """
    Obtiene datos desde la API y los convierte en DataFrame normalizado.
    Hace una solicitud GET
    Extrae y transforma los campos necesarios
    Normaliza las columnas a minúsculas
    Convierte crash_date a tipo datetime    """
    
    try:
        response = requests.get(API_URL)
        response.raise_for_status()
        data = response.json()
    except Exception as e:
        print(f"Problemas al obtener datos de la API: {e}")
        return pd.DataFrame()

    person_data = [
        {
            'crash_record_id': item.get('crash_record_id'),
            'person_id': item.get('person_id'),
            'vehicle_id': item.get('vehicle_id'),
            'crash_date': item.get('crash_date'),
            'city': item.get('city'),
            'state': item.get('state'),
            'zipcode': item.get('zipcode'),
            'sex': item.get('sex'),
            'age': item.get('age'),
            'drivers_license_state': item.get('drivers_license_state'),
            'safety_equipment': item.get('safety_equipment'),
            'airbag_deployed': item.get('airbag_deployed'),
            'ejection': item.get('ejection'),
            'injury_classification': item.get('injury_classification'),
            'driver_action': item.get('driver_action'),
            'driver_vision': item.get('driver_vision'),
            'physical_condition': item.get('physical_condition'),
            'bac_result': item.get('bac_result'),
            'bac_result_value': item.get('bac_result_value'),
            'unit_type': item.get('unit_type'),
            'person_type': item.get('person_type'),
            'person_num': item.get('person_num'),
            'injury_type': item.get('injury_type'),
            'hospital': item.get('hospital'),
            'ems_run_no': item.get('ems_run_no'),
            'ems_agency': item.get('ems_agency'),
            'ems_response_time': item.get('ems_response_time'),
            'ems_transport_time': item.get('ems_transport_time'),
            'ems_hospital_time': item.get('ems_hospital_time'),
        }
        for item in data
        if item.get('crash_record_id') and item.get('person_id')
    ]

    df = pd.DataFrame(person_data)
    df.columns = df.columns.str.lower()
    df['crash_date'] = pd.to_datetime(df['crash_date'], errors='coerce')
    return df

def guardar_csv(df, path, fecha):
    """
    Guarda el DataFrame en un archivo .csv con nombre basado en la fecha actual.
    Crea el directorio si no existe
    Define nombre y ruta del archivo
    Exporta usando df.to_csv    """

    try:
        os.makedirs(path, exist_ok=True)
        nombre = f"chicago_reportCrash_people_{fecha.strftime('%Y_%m_%d')}.csv"
        ruta = os.path.join(path, nombre)
        df.to_csv(ruta, index=False, sep=';', encoding='utf-8')
        print(f"Datos exportados a {ruta}")
        return ruta
    except Exception as e:
        print(f"Problemas al guardar CSV: {e}")
        return None

def subir_a_cos(ruta_archivo, bucket, nombre_objeto, apikey, resource_instance_id, endpoint, carpeta_destino="Chicago/People"):
    """
    Sube un archivo a IBM Cloud Object Storage en una carpeta específica.
    Si la carpeta no existe, la crea automáticamente.
    
    Args:
        ruta_archivo: Ruta local del archivo a subir
        bucket: Nombre del bucket en COS
        nombre_objeto: Nombre del archivo en COS
        apikey: API key de IBM Cloud
        resource_instance_id: ID de la instancia de COS
        endpoint: Endpoint de COS
        carpeta_destino: Ruta de la carpeta donde guardar (por defecto: Chicago/People)
    """
    try:
        print("Intentando subir a COS...")
        cos = ibm_boto3.client("s3",
            ibm_api_key_id=apikey,
            ibm_service_instance_id=resource_instance_id,
            config=Config(signature_version="oauth"),
            endpoint_url=endpoint
        )
        
        # Construir la ruta completa en COS (carpeta/archivo)
        ruta_completa = f"{carpeta_destino}/{nombre_objeto}"
        
        # Verificar si la carpeta existe, si no, crearla
        try:
            # Intentar listar objetos en la carpeta para verificar si existe
            response = cos.list_objects_v2(
                Bucket=bucket,
                Prefix=f"{carpeta_destino}/",
                MaxKeys=1
            )
            
            # Si no hay objetos en la carpeta, crear un archivo vacío para "crear" la carpeta
            if 'Contents' not in response or len(response['Contents']) == 0:
                print(f"Creando carpeta '{carpeta_destino}' en COS...")
                cos.put_object(
                    Bucket=bucket,
                    Key=f"{carpeta_destino}/.keep",
                    Body=""
                )
                print(f"Carpeta '{carpeta_destino}' creada exitosamente.")
            else:
                print(f"Carpeta '{carpeta_destino}' ya existe en COS.")
                
        except Exception as e:
            print(f"Error al verificar/crear carpeta: {e}")
            # Continuar con la subida del archivo de todas formas
        
        # Subir el archivo a la carpeta especificada
        with open(ruta_archivo, "rb") as archivo:
            cos.upload_fileobj(archivo, bucket, ruta_completa)
        
        print(f"Archivo subido a COS: {ruta_completa}")
        return True
        
    except Exception as e:
        print(f"Problemas al subir a COS: {e}")
        return False

def main(path: str):
    """Ejecuta el flujo principal: descarga de datos, inserción en la base, y guardado como CSV si hay nuevos registros.
    Llama a obtener_datos
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
                ruta_txt = os.path.join(path, 'csv_path_people.txt')
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
                # === Carpeta donde guardar los archivos en COS ===
                CARPETA_DESTINO = "Chicago/People"
                # ============================================================
                subir_a_cos(ruta_csv, BUCKET, NOMBRE_OBJETO, APIKEY, RESOURCE_INSTANCE_ID, ENDPOINT, CARPETA_DESTINO)
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
