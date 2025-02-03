import sys
sys.path.insert(0, r'../../db_configuration/')
sys.path.insert(0, r'../../utils/')
sys.path.insert(0, r'..')
from    connectiondb        import sqlConnectDB
from    async_manager       import workloadAsync, getConfigRpa

import  json
import  pandas              as pd
from    logger              import setup_logger
from    datetime            import datetime, timedelta
from    dotenv              import load_dotenv
import  asyncio
import  os
import  aiohttp
import  aioodbc
import  pyfiglet
from    prefect             import task, flow


load_dotenv()

db_server                           =           os.getenv("db_server")
db_database                         =           os.getenv("db_database")

baseFolder                          =           r'../../data/03_TARGETDATA/'
baseFolderOriginal                  =           r'../../data/01_DNE/GoAnalytics/'

banner                              =           pyfiglet.figlet_format(f"goAnalytics-campaing")
header                              =           f"{'=' * 50}\n{banner}{'=' * 10}\nIniciando workflow Agentes"

currentDate                         =           datetime.now().strftime("%Y%m%d")
timeExecuting                       =           datetime.now().strftime("%Y-%m-%d %H:%M:%S")





def setupLoggerTask(mode):
    logFileName                     =           f'../../logs/GoAnalytics-Api/GoAnalytics-Api_{currentDate}.log'
    return setup_logger(log_file=logFileName, logger_name=f'GoAnalytics-Api_{currentDate}')

logger                              =           setupLoggerTask(mode="campaing")

@task(log_prints=True, cache_key_fn=lambda *args, **kwargs: None)
async def getConfiguration():


    logger.info("Conectando a la base de datos para obtener configuraciones...")
    print("Conectando a la base de datos para obtener configuraciones...")

    configurations = []
    connStr        = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={db_server};DATABASE={db_database};Trusted_Connection=yes;"

    query          = """
    SELECT DISTINCT 
        userDomain,
        passColumn,
        campaing,
        SUBSTRING(userDomain, CHARINDEX('@', userDomain), LEN(userDomain)) AS domaingService,
        url + '/poll/auth/token' AS urlAuth,
        url + '/poll/api/goanalytics/campaigns' AS epAgents
    FROM tbl_configuration_gocontact
    WHERE userDomain IS NOT NULL;
    """

    try:
        async with aioodbc.connect(dsn=connStr) as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(query)
                rows = await cursor.fetchall()

                for row in rows:
                    configurations.append({
                        "userDomain": row[0],
                        "passColumn": row[1],
                        "campaing": row[2],
                        "domaingService": row[3],
                        "urlAuth": row[4],
                        "epAgents": row[5],
                    })
        logger.info("Configuraciones obtenidas exitosamente.")
        print("Configuraciones obtenidas exitosamente.")
    except Exception as e:
        logger.error(f"Error al obtener configuraciones: {e}")
        print(f"Error al obtener configuraciones: {e}")
    return configurations

@task(log_prints=True, cache_key_fn=lambda *args, **kwargs: None)
async def getToken(session, config):


    try:
        logger.info(f"Obteniendo token para {config['userDomain']}...")
        print(f"Obteniendo token para {config['userDomain']}...")
        async with session.post(config["urlAuth"], auth=aiohttp.BasicAuth(config["userDomain"], config["passColumn"])) as response:
            response.raise_for_status()
            data = await response.json()
            token = data.get("token")
            if token:
                logger.info(f"Token obtenido para {config['userDomain']}")
                print(f"Token obtenido para {config['userDomain']}")
                return {"userDomain": config["userDomain"], "token": token}
            else:
                logger.error(f"No se pudo obtener un token para {config['userDomain']}.")
                print(f"No se pudo obtener un token para {config['userDomain']}.")
                return None
    except Exception as e:
        logger.error(f"Error al obtener token para {config['userDomain']}: {e}")
        print(f"Error al obtener token para {config['userDomain']}: {e}")
        return None

@task(log_prints=True, cache_key_fn=lambda *args, **kwargs: None)
async def getApi(session, endpoint, token, config):


    logger.info(f"Descargando datos de agentes para {config['campaing']} ({config['userDomain']})...")
    print(f"Descargando datos de agentes para {config['campaing']} ({config['userDomain']})...")
    headers = {"Authorization": f"Bearer {token}"}

    try:
        async with session.get(endpoint, headers=headers) as response:
            response.raise_for_status()
            data = await response.json()

            if not isinstance(data, list):
                logger.error(f"Datos recibidos no son una lista: {data}")
                print(f"Datos recibidos no son una lista: {data}")
                return []

            # add new columns to the data
            for record in data:
                if isinstance(record, dict):
                    record["campaing"] = config["campaing"]
                    ##record["domaing"] = config["domaingService"]
                else:
                    logger.warning(f"Registro no esperado: {record}")
                    print(f"Registro no esperado: {record}")

            return data
    except Exception as e:
        logger.error(f"Error al descargar datos para {config['campaing']}: {e}")
        print(f"Error al descargar datos para {config['campaing']}: {e}")
        return []

@flow(log_prints=True)
async def workFlow_ApiDimCampaingGA(save_mode="consolidated"):

    logger.info(header)
    print(header)

    configurations = await getConfiguration()
    if not configurations:
        logger.error("No se encontraron configuraciones.")
        print("No se encontraron configuraciones.")
        return

    allDataTarget = []
    async with aiohttp.ClientSession() as session:
        tokenTask = [getToken(session, config) for config in configurations]
        tokens = await asyncio.gather(*tokenTask)

        for tokenInfo, config in zip(tokens, configurations):
            if tokenInfo and tokenInfo.get("token"):
                data = await getApi(session, config["epAgents"], tokenInfo["token"], config)
                allDataTarget.extend(data)

        if save_mode == "consolidated" and allDataTarget:
            # Guardar todo en un único archivo consolidado
            df = pd.DataFrame(allDataTarget)
            
            output_file = os.path.join(baseFolder, f"DimCampaing.txt")
            os.makedirs(baseFolder, exist_ok=True)
            df.to_csv(output_file, index=False, sep='|')
            logger.info(f"Datos consolidados guardados en un único archivo: '{output_file}'")
            print(f"Datos consolidados guardados en un único archivo: '{output_file}'")
            
            
            output_json_file = os.path.join(baseFolderOriginal, f"DimCampaing.json")
            df.to_json(output_json_file, orient='records',indent=4)
            
            logger.info(f"Datos consolidados guardados en un archivo JSON: '{output_json_file}'")
            print(f"Datos consolidados guardados en un archivo JSON: '{output_json_file}'")

            query = '''
            SELECT dboName, nameFile , typeFile, delimiter, chunkSize, sp_proccess, db_database, db_server, if_exists, campaing, filePath
            FROM tbl_configuration_workloads_bi WHERE id = 10
            '''

            settings = getConfigRpa(query)
            first_key = next(iter(settings))
            serviceName = settings[first_key]['name_procces']

            db_config = {
                'server': os.getenv('db_server'),
                'database': os.getenv('db_database')
            }

            await workloadAsync(
                serviceName=serviceName,
                db_config=db_config,
                logCfg={f'../../logs/{serviceName}'},
                sp_sync_execution=False,
                config_rpa=settings
            )

if __name__ == "__main__":
    asyncio.run(workFlow_ApiDimCampaingGA("consolidated"))
