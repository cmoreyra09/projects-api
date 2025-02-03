import    requests
import    os
from      dotenv         import load_dotenv
import    json
import    datetime
import    pandas         as pd
import    asyncio
from      prefect        import task, flow
from      sqlalchemy     import text

import              sys
sys.path.insert(0, '..')
sys.path.insert(0, '../../db_configuration/')
sys.path.insert(0, '../../utils/')

from      connectiondb   import sqlConnectDB
from      async_manager  import workloadAsync, getConfigRpa
from      logger         import setup_logger
import    pyfiglet
import    time


load_dotenv()


db_server                       =               os.getenv('db_server')
db_database                     =               os.getenv('db_database') 

db                              =               sqlConnectDB(db_server,db_database)
engine                          =               db.connectDb()

# Fechas para el procesamiento
date_reprocessing               =               datetime.date.today() - datetime.timedelta(1)
today                           =               datetime.date.today()

# Date Request
currentDate                     =               today.strftime("%Y%m%d")


timeExecuting                   =               today.strftime("%Y-%m-%d %H:%M:%S")

banner                          =               pyfiglet.figlet_format(f"WORKFLOW API GO OCULAR")
header                          =               f"{'=' * 50}\n{banner}{'=' * 10}\nIniciando workflow ApiGoContact - {timeExecuting}"




with engine.connect() as connection:


    credentialsApi              =               connection.execute(text('SELECT * FROM Tbl_configuration_api WHERE Id = 1')).fetchone()
    credentialsBI               =               connection.execute(text('SELECT * FROM tbl_configuration_workloads_bi WHERE Id = 1')).fetchone()

    serviceName                 =               credentialsBI[3]
    domain                      =               credentialsApi[2]
    token                       =               credentialsApi[3]
    userName                    =               credentialsApi[4]
    nameFile                    =               credentialsBI[5]
    Id                          =               credentialsBI[0]  

db.closeConnect()


def setupLogger(serviceName,logCfg):
    return setup_logger(log_file=f'../../logs/{serviceName}/{serviceName}_{currentDate}.log',logger_name=serviceName)

logger = setupLogger(serviceName, logCfg={})

print(header)
logger.info(header)

@task(log_prints=True)
def extraction(page=1, accumulator=[]):
    
    try:
        response = requests.get(domain, params={'page': page,'timeZone': 'America/Lima','start': date_reprocessing,'end': today },headers={ 'X-Requested-With': userName,'Authorization': token })

        if response.status_code == 200:
            response.raise_for_status() 

            data = response.json()

            if not data or 'data' not in data or 'data' not in data['data']:
                return accumulator

            dataset = data['data']['data']
            items = data['data']['countItems']

            accumulator.extend(dataset)

            print(f'Currently Page: {page} / Items: {items}')
            logger.info(f'Currently Page: {page} / Items: {items}')

            if len(dataset) > 0:
                return extraction(page + 1, accumulator)

            print('\nDetalle Final API.\n')
            logger.info('\nDetalle Final API.\n')
            print(json.dumps(data, indent=4))
            logger.info(json.dumps(data, indent=4))
            print('.------------------------------------------')
            logger.info('.------------------------------------------')
            
            return accumulator
        else:
            print(f"Error: Received unexpected status code {response.status_code}")
            logger.error(f"Error: Received unexpected status code {response.status_code}")
            return accumulator

    except requests.exceptions.RequestException as e:
        print(f"Error fetching data: {e}")
        logger.error(f"Error fetching data: {e}")
        return accumulator

@task(log_prints=True)
def transformation(data):

    base_path = f'../../data/01_DNE/{serviceName}'
    print(f'Base path: {base_path}')
    logger.info(f'Base path: {base_path}')

    json_file_path = f'{base_path}/{nameFile}_{currentDate}.json'
    print(f'JSON file path: {json_file_path}')
    logger.info(f'JSON file path: {json_file_path}')

    de_path = f'../../data/02_DE/{serviceName}'
    print(f'DE path: {de_path}')
    logger.info(f'DE path: {de_path}')

    de_file_path = f'{de_path}/DE_{nameFile}_{currentDate}.txt'
    print(f'DE file path: {de_file_path}')
    logger.info(f'DE file path: {de_file_path}')

    target_data_path = '../../data/03_TARGETDATA'
    print(f'Target data path: {target_data_path}')

    target_data_file_path = f'{target_data_path}/{nameFile}.txt'
    logger.info(f'Target data file path: {target_data_file_path}')

    if not os.path.exists(base_path):
        os.makedirs(base_path)
        print(f'Base path created.')
        logger.info(f'Base path created.')
    else:
        print(f'Base path already exists.')
        logger.info(f'Base path already exists.')
    
    if not os.path.exists(de_path):
        os.makedirs(de_path)
        print(f'DE path created.')
        logger.info(f'DE path created.')
    else:
        print(f'DE path already exists.')
        logger.info(f'DE path already exists.')

    # Guardar los datos en un archivo JSON
    with open(json_file_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=4)
    print(f'Data saved in DNE.')
    logger.info(f'Data saved in DNE.')

    # Leer el archivo JSON como una cadena
    with open(json_file_path, 'r', encoding='utf-8') as file:
        json_str = file.read()
    print(f'File readed.')
    logger.info(f'File readed.')

    # Convertir la cadena JSON a un objeto Python
    data = json.loads(json_str)
    print(f'JSON loaded.')
    logger.info(f'JSON loaded.')

    # Normalizar el JSON
    df = pd.json_normalize(data)
    print(f'JSON normalized.')
    logger.info(f'JSON normalized.')

    print(f'Dataframe head: {df.head()}')
    logger.info(f'Dataframe head: {df.head()}')

    # Guardar los datos en archivos CSV
    df.to_csv(de_file_path, sep='|', index=False)
    print(f'Data saved in DE.')
    logger.info(f'Data saved in DE.')

    df.to_csv(target_data_file_path, sep='|', index=False)
    print(f'Data saved')
    logger.info(f'Data saved')

    sqlQuery = f'''
    SELECT dboName, nameFile , typeFile, delimiter, chunkSize, sp_proccess, db_database, db_server, if_exists, campaing, filePath FROM tbl_configuration_workloads_bi WHERE id = {Id}
    '''
    print(f'SQL Query: {sqlQuery}')
    logger.info(f'SQL Query: {sqlQuery}')

    return sqlQuery

@task(log_prints=True)
def loadIngest(sqlQuery):
    settings = getConfigRpa(sqlQuery)
    first_key = next(iter(settings))
    serviceName = settings[first_key]['name_procces']

    db_config = {'server': db_server,'database': db_database}

    asyncio.run(workloadAsync(serviceName=serviceName,db_config=db_config,logCfg={f'../../logs/{serviceName}'},sp_sync_execution=False,config_rpa=settings))


@flow(log_prints=True)
def workFlow_ApiOcularNatura():
    data = extraction()
    sqlQuery = transformation(data)
    loadIngest(sqlQuery)

if __name__ == '__main__':
    workFlow_ApiOcularNatura()
