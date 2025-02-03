import    requests
import    os
from      dotenv         import load_dotenv
import    json
import    datetime
import    pandas         as pd
import    asyncio
from      prefect        import task, flow
from      sqlalchemy     import text
import    base64
from      os             import listdir
import    sys
sys.path.insert(0, '..')
sys.path.insert(0, '../../db_configuration/')
sys.path.insert(0, '../../utils/')
from      connectiondb   import sqlConnectDB
from      async_manager  import workloadAsync, getConfigRpa
from      logger         import setup_logger
import    pyfiglet

load_dotenv()


db_server                       =               os.getenv('db_server')
db_database                     =               os.getenv('db_database')

db = sqlConnectDB(db_server,db_database)
engine = db.connectDb()


# Fechas para el procesamiento
date_reprocessing               =               datetime.date.today() - datetime.timedelta(1)
today                           =               datetime.date.today()

# Date Request
currentDate                     =               today.strftime("%Y%m%d")
timeExecuting                   =               datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")


banner                          =               pyfiglet.figlet_format(f"WORKFLOW API GO LimeSurvey")
header                          =               f"{'=' * 50}\n{banner}{'=' * 10}\nIniciando workflow ApiGoContact - {timeExecuting}"


with engine.connect() as connection:

    listId                      =               connection.execute(text('SELECT IdEncuesta FROM T_Configurador_LimeSurvey WHERE StateId = 1')).fetchall()
    credentialsApi              =               connection.execute(text('SELECT * FROM Tbl_configuration_api WHERE Id = 4')).fetchone()
    credentialsBi               =               connection.execute(text('SELECT * FROM tbl_configuration_workloads_bi WHERE Id = 5')).fetchone()
    listFiles                   =               connection.execute(text("SELECT DISTINCT(nameFile) FROM tbl_configuration_workloads_bi WHERE campaing = 'LimeSurvey'")).fetchall()
    surveys                     =               []
    files                       =               []   


    for i in listId:
        surveys.append(i[0])
    
    for i in listFiles:
        files.append(i[0])

    surveysId                   =               surveys
    filesName                   =               files
    domain                      =               credentialsApi[2]
    user                        =               credentialsApi[4]  
    password                    =               credentialsApi[5]
    serviceName                 =               credentialsBi[3] 


def setupLogger(serviceName,logCfg):
    return setup_logger(log_file=f'../../logs/{serviceName}/{serviceName}_{currentDate}.log',logger_name=serviceName)

logger = setupLogger(serviceName, logCfg={})

print(header)
logger.info(header)

@task(log_prints=True)
def extraction():


    payload = json.dumps({"method": "get_session_key", "params": [user, password], "id": 1})
    headers = {'Content-Type': 'application/json'}
    response = requests.post(domain, headers=headers, data=payload)

    session_key = response.json().get('result')
    if not session_key:
        print("Error al obtener la clave de sesión:", response.json().get('error', 'No disponible'))
        logger.error(f"Error al obtener la clave de sesión: {response.json().get('error', 'No disponible')}")
        exit()

    ruta_guardado = f'../../data/01_DNE/{serviceName}'

    # Crear la carpeta si no existe
    os.makedirs(ruta_guardado, exist_ok=True)

    '''
    # Eliminar archivos JSON existentes en la ruta de guardado
    for file_name in listdir(ruta_guardado):
        if file_name.endswith('.json'):
            os.remove(os.path.join(ruta_guardado, file_name))
    '''

    # Procesar cada encuesta en la lista de survey_ids
    for survey_id in surveysId:
        print(f"Procesando encuesta con ID: {survey_id}")
        logger.info(f"Procesando encuesta con ID: {survey_id}")

        payload = json.dumps({"method": "export_responses", "params": [session_key, survey_id, "json", None, "complete", "full", "long"], "id": 1})
        response = requests.post(domain, headers=headers, data=payload)

        if response.status_code == 200 and 'result' in response.json():
            survey_responses_encoded = response.json().get('result')
            if isinstance(survey_responses_encoded, str):
                survey_data = base64.b64decode(survey_responses_encoded).decode('utf-8')
                file_name = f"results_survey_{survey_id}.json"
                file_path = os.path.join(ruta_guardado, file_name)
                with open(file_path, 'w', encoding='utf-8') as f:
                    json.dump(json.loads(survey_data), f, ensure_ascii=False, indent=4)
            else:
                print(f"Datos no válidos para la encuesta con ID {survey_id}.")
                logger.error(f"Datos no válidos para la encuesta con ID {survey_id}.")
        else:
            print(f"Encuesta no encontrada con ID {survey_id}. Respuesta HTTP: {response.status_code}")
            logger.error(f"Encuesta no encontrada con ID {survey_id}. Respuesta HTTP: {response.status_code}")

    print("Proceso completado exitosamente.")
    logger.info("Proceso completado exitosamente.")
    
    return ruta_guardado

@task(log_prints=True)
def transformation(filepathSaved):
    target_data_path = r'../../data/03_TARGETDATA'

    # Crear la carpeta de destino si no existe
    os.makedirs(target_data_path, exist_ok=True)

    # Leer todos los archivos JSON en la carpeta ruta_guardado
    for file_name in listdir(filepathSaved):
        if file_name.endswith('.json'):
            file_path = os.path.join(filepathSaved, file_name)
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # Extraer la lista de respuestas
            responses = data.get('responses', [])
            
            # Convertir la lista de respuestas a un DataFrame de pandas
            df = pd.json_normalize(responses)
            
            # Truncar los nombres de las columnas a 13 caracteres
            df.columns = [col[:13] for col in df.columns]
            
            # Guardar el DataFrame como un archivo CSV en la carpeta de destino
            target_file_path = os.path.join(target_data_path, file_name.replace('.json', '.txt'))
            df.to_csv(target_file_path, sep='|', index=False)
            print(f"Archivo '{target_file_path}' guardado.")
            logger.info(f"Archivo '{target_file_path}' guardado.")

    sqlQuery = f'''
        SELECT dboName, nameFile , typeFile, delimiter, chunkSize, sp_proccess, db_database, db_server, if_exists, campaing, filePath FROM tbl_configuration_workloads_bi WHERE campaing = 'LimeSurvey'
        '''

    return sqlQuery

@task(log_prints=True)
def loadIngest(sqlQuery):
    settings = getConfigRpa(sqlQuery)
    first_key = next(iter(settings))
    serviceName = settings[first_key]['name_procces']

    db_config = {'server': db_server,'database': db_database}

    asyncio.run(workloadAsync(serviceName=serviceName,db_config=db_config,logCfg={f'../../logs/{serviceName}'},sp_sync_execution=False,config_rpa=settings))

@flow(log_prints=True)
def workFlow_ApiLimeSurveyEncuestas():
    data = extraction()
    sqlQuery = transformation(data)
    loadIngest(sqlQuery)

if __name__ == '__main__':
    workFlow_ApiLimeSurveyEncuestas()
