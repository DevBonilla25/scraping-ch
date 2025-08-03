# Chicago Data Scraper

Este proyecto extrae datos de accidentes de tráfico de la API de Chicago y los almacena en una base de datos PostgreSQL.

## Configuración

### Variables de Entorno

Para que el script funcione correctamente, necesitas configurar las siguientes variables de entorno:

```bash
# Configuración de la base de datos PostgreSQL (requeridas)
DB_HOST=your-postgresql-host
DB_PORT=5432
DB_NAME=your-database-name
DB_USER=your-username
DB_PASSWORD=your-password

# Configuración de IBM Cloud Object Storage (opcionales)
COS_API_KEY=your-cos-api-key
COS_RESOURCE_INSTANCE_ID=your-resource-instance-id
COS_ENDPOINT=https://s3.us-south.cloud-object-storage.appdomain.cloud
COS_BUCKET=your-bucket-name
```

**Nota**: Si no configuras las variables de COS, el script seguirá funcionando pero no subirá archivos a Cloud Object Storage.

### En Code Engine

Si estás ejecutando este script en IBM Code Engine, puedes configurar las variables de entorno en la configuración del job o aplicación:

1. Ve a tu aplicación/job en Code Engine
2. En la sección de configuración, agrega las variables de entorno necesarias
3. Asegúrate de que la base de datos PostgreSQL esté accesible desde Code Engine

## Funcionalidades

- **Extracción de datos**: Obtiene datos de accidentes de tráfico desde la API de Chicago
- **Transformación**: Normaliza y limpia los datos
- **Carga**: Inserta los datos en una base de datos PostgreSQL
- **Almacenamiento**: Guarda los datos como archivos CSV y los sube a IBM Cloud Object Storage

## Estructura de la base de datos

El script crea automáticamente la tabla `chicago_crashes_people` con los siguientes tipos de datos:

- `crash_date`: TIMESTAMP
- `age`: INTEGER
- `person_num`: INTEGER
- `ems_response_time`: INTEGER
- `ems_transport_time`: INTEGER
- `ems_hospital_time`: INTEGER
- `bac_result_value`: DECIMAL(5,3)
- Otros campos: TEXT

## Ejecución

```bash
python chicago-test.py
```

El script:
1. Extrae datos de la API de Chicago
2. Los inserta en la base de datos PostgreSQL
3. Guarda un archivo CSV si se insertaron nuevos registros
4. Sube el archivo CSV a IBM Cloud Object Storage 