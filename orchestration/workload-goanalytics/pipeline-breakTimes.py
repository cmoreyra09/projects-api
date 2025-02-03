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



db_server                           =           os.getenv("db_server")
db_database                         =           os.getenv("db_database")

baseFolder                          =           r'../../data/03_TARGETDATA/'
baseFolderOriginal                  =           r'../../data/01_DNE/GoAnalytics/'
baseFolderDE                        =           r'../../data/02_DE/GoAnalytics/'



banner                              =           pyfiglet.figlet_format(f"goAnalytics-BreakTimes")
header                              =           f"{'=' * 50}\n{banner}{'=' * 10}\nIniciando workflow Agentes"

currentDate                         =           datetime.now().strftime("%Y%m%d")
timeExecuting                       =           datetime.now().strftime("%Y-%m-%d %H:%M:%S")





def setupLoggerTask(mode):
    logFileName                     =           f'../../logs/GoAnalytics-Api/GoAnalytics-Api_{currentDate}.log'
    return setup_logger(log_file=logFileName, logger_name=f'GoAnalytics-Api_{currentDate}')

logger                              =           setupLoggerTask(mode="BreakTimes")



@task(log_prints=True, cache_key_fn=lambda *args, **kwargs: None)
async def getConfiguration(specific_date):


    logger.info("Conectando a la base de datos para obtener configuraciones...")
    print("Conectando a la base de datos para obtener configuraciones...")

    configurations = []
    connStr = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={db_server};DATABASE={db_database};Trusted_Connection=yes;"

    query = """
    SELECT DISTINCT 
        userDomain,
        passColumn,
        campaing,
        SUBSTRING(userDomain, CHARINDEX('@', userDomain), LEN(userDomain)) AS domaingService,
        url + '/poll/auth/token' AS urlAuth,
        url + '/poll/api/goanalytics/total/breaks/' AS epTimeLine
    FROM tbl_configuration_gocontact
    WHERE userDomain IS NOT NULL;
    """

    startDate = f"{specific_date}T00:00:00"
    endDate = f"{specific_date}T24:00:00"

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
                        "epTimeLine": row[5],
                        "startDate": startDate,
                        "endDate": endDate
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
async def getApi(session, endpoint, token, config, specific_date):


    logger.info(f"Descargando datos de timeline para {config['campaing']} ({config['userDomain']})...")
    print(f"Descargando datos de timeline para {config['campaing']} ({config['userDomain']})...")
    headers = {"Authorization": f"Bearer {token}"}
    url = f"{endpoint}{config['startDate']}/{config['endDate']}"
    params = {"by": "break_name,agent"}

    try:
        async with session.get(url, headers=headers, params=params) as response:
            response.raise_for_status()
            data = await response.json()

            if not isinstance(data, list):
                logger.error(f"Datos recibidos no son una lista: {data}")
                print(f"Datos recibidos no son una lista: {data}")
                return []

            for record in data:
                if isinstance(record, dict):
                    record["request_date"] = specific_date.replace('-', '')  # Usar specific_date en formato YYYYMMDD
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
async def workFlow_ApiBreakTimesGA(date_range, save_mode="separate"):
    """
    Proceso principal para ejecutar el flujo con un rango de fechas.
    """
    logger.info(header)
    print(header)

    allDataTarget = []
    async with aiohttp.ClientSession() as session:
        for specific_date in date_range:
            logger.info(f"Procesando la fecha: {specific_date}")
            print(f"Procesando la fecha: {specific_date}")

            configurations = await getConfiguration(specific_date)
            if not configurations:
                logger.error(f"No se encontraron configuraciones para la fecha: {specific_date}")
                print(f"No se encontraron configuraciones para la fecha: {specific_date}")
                continue

            tokenTask = [getToken(session, config) for config in configurations]
            tokens = await asyncio.gather(*tokenTask)

            for tokenInfo, config in zip(tokens, configurations):
                if tokenInfo and tokenInfo.get("token"):
                    data = await getApi(session, config["epTimeLine"], tokenInfo["token"], config, specific_date)
                    allDataTarget.extend(data)

            if save_mode == "separate" and allDataTarget:
                
                df = pd.DataFrame(allDataTarget)
                output_file = os.path.join(baseFolder, f"BreakTime_{specific_date.replace('-', '')}.csv")
                os.makedirs(baseFolder, exist_ok=True)
                df.to_csv(output_file, index=False, sep="|")
                logger.info(f"Datos consolidados guardados en '{output_file}'")
                print(f"Datos consolidados guardados en '{output_file}'")
                allDataTarget = []  # Reset para la siguiente fecha

    if save_mode == "consolidated" and allDataTarget:
        # Guardar todo en un único archivo consolidado
        df = pd.DataFrame(allDataTarget)
        output_file = os.path.join(baseFolder, f"BreakTime.txt")
        os.makedirs(baseFolder, exist_ok=True)
        df.to_csv(output_file, index=False,sep="|")
        
        df = pd.DataFrame(allDataTarget)
        output_file = os.path.join(baseFolderDE, f"BreakTime_{currentDate}.txt")
        os.makedirs(baseFolder, exist_ok=True)
        df.to_csv(output_file, index=False,sep="|")

        logger.info(f"Datos consolidados guardados en un único archivo: '{output_file}'")
        print(f"Datos consolidados guardados en un único archivo: '{output_file}'")
        
        output_filejson = os.path.join(baseFolderOriginal, f"BreakTime_{currentDate}.json") 
        os.makedirs(baseFolder, exist_ok=True)
        df.to_json(output_filejson, orient='records', indent=4)

        query = '''
        SELECT dboName, nameFile , typeFile, delimiter, chunkSize, sp_proccess, db_database, db_server, if_exists, campaing, filePath
        FROM tbl_configuration_workloads_bi WHERE id = 8
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
    
    # Generar rango de fechas (por ejemplo, del 2024-11-10 al 2024-11-15)
    start_date = (datetime.now() - timedelta(days=3)).strftime("%Y-%m-%d")
    end_date = datetime.now().strftime("%Y-%m-%d")
    
    date_range = [str((datetime.strptime(start_date, "%Y-%m-%d") + timedelta(days=i)).date()) for i in range((datetime.strptime(end_date, "%Y-%m-%d") - datetime.strptime(start_date, "%Y-%m-%d")).days + 1)]

    asyncio.run(workFlow_ApiBreakTimesGA(date_range, "consolidated"))
