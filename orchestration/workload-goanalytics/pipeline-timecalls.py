import sys
sys.path.insert(0, r'../../db_configuration/')
sys.path.insert(0, r'../../utils/')
sys.path.insert(0, r'..')
from            connectiondb    import sqlConnectDB
from            async_manager   import workloadAsync, getConfigRpa

import          json
import          pandas          as pd
from            logger          import setup_logger
from            datetime        import datetime, timedelta
from            dotenv          import load_dotenv
import          asyncio
import          os
import          aiohttp
import          aioodbc
import          pyfiglet
from            prefect         import task, flow

load_dotenv()


db_server                       =           os.getenv("db_server")

db_database                     =           os.getenv("db_database")

baseFolder                      =           r'../../data/03_TARGETDATA/'

originalDataFolder              =           r'../../data/01_DNE/GoAnalytics/'

baseFolderDE                    =           r'../../data/02_DE/GoAnalytics/'


currentDate                     =           datetime.now().strftime("%Y%m%d")

timeExecuting                   =           datetime.now().strftime("%Y-%m-%d %H:%M:%S")

banner                          =           pyfiglet.figlet_format(f"goAnalytics-Calls-Timeline")

header                          =           f"{'=' * 50}\n{banner}{'=' * 10}\nIniciando workflow ApiGoContact - {timeExecuting}"


def setupLoggerTask(mode):
    logFileName = f'../../logs/GoAnalytics-Api/GoAnalytics-Api_{currentDate}.log'
    return setup_logger(log_file=logFileName, logger_name=f'GoAnalytics-Api_{currentDate}')


logger = setupLoggerTask(mode="timelineCalls")


@task(log_prints=True, cache_key_fn=lambda *args, **kwargs: None)
def transformDate(data):
    logger.info("Iniciando transformación de fechas en los datos...")
    print("Iniciando transformación de fechas en los datos...")
    for record in data:
        try:
            if "min_stamp" in record and record["min_stamp"]:
                record["min_stamp"] = datetime.strptime(record["min_stamp"], "%Y-%m-%dT%H:%M").strftime("%Y-%m-%d %H:%M:%S")
            if "max_stamp" in record and record["max_stamp"]:
                record["max_stamp"] = datetime.strptime(record["max_stamp"], "%Y-%m-%dT%H:%M").strftime("%Y-%m-%d %H:%M:%S")
            if "stamp" in record and record["stamp"]:
                record["stamp"] = datetime.strptime(record["stamp"], "%Y-%m-%dT%H:%M").strftime("%Y-%m-%d %H:%M:%S")
            if 'series' in record and record['series']:
                record['series'] = datetime.strptime(record['series'], "%Y-%m-%dT%H:%M:%S.%fZ").strftime("%Y-%m-%d %H:%M:%S")
            if 'call_stamped' in record and record['call_stamped']:
                record['call_stamped'] = datetime.strptime(record['call_stamped'], "%Y-%m-%dT%H:%M").strftime("%Y-%m-%d %H:%M:%S")
            if 'day_stamp' in record and record['day_stamp']:
                record['day_stamp'] = datetime.strptime(record['day_stamp'], "%Y-%m-%dT%H:%M").strftime("%Y-%m-%d %H:%M:%S")
        except Exception as e:
            logger.error(f"Error al transformar fechas en el registro: {e}")
            print(f"Error al transformar fechas en el registro: {e}")
    logger.info("Transformación de fechas completada.")
    print("Transformación de fechas completada.")
    return data


@task(log_prints=True, cache_key_fn=lambda *args, **kwargs: None)
def calculateDates(mode):
    logger.info(f"Calculando fechas según el modo: {mode}...")
    print(f"Calculando fechas según el modo: {mode}...")

    currentDate = datetime.now()

    if mode == "online":
        startDate = (currentDate - timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%S")
        endDate = currentDate.strftime("%Y-%m-%dT%H:%M:%S")

        logger.info(f"startDate={startDate}, endDate={endDate}")
        print(f"startDate={startDate}, endDate={endDate}")

    elif mode == "offline":
        startDate = (currentDate - timedelta(days=13)).strftime("%Y-%m-%dT00:00:00")
        endDate = currentDate.replace(hour=23, minute=59, second=59, microsecond=0).strftime("%Y-%m-%dT%H:%M:%S")

        logger.info(f"startDate={startDate}, endDate={endDate}")
        print(f"startDate={startDate}, endDate={endDate}")

    elif mode == "cutDay":
        startDate = currentDate.replace(hour=0, minute=0, second=0, microsecond=0).strftime("%Y-%m-%dT%H:%M:%S")
        endDate = currentDate.replace(hour=23, minute=59, second=59, microsecond=0).strftime("%Y-%m-%dT%H:%M:%S")

        logger.info(f"startDate={startDate}, endDate={endDate}")
        print(f"startDate={startDate}, endDate={endDate}")

    else:
        error_msg = "Modo inválido. Debe ser 'online', 'offline' o 'cutDay'."
        logger.error(error_msg)
        print(error_msg)
        raise ValueError(error_msg)

    logger.info(f"Fechas calculadas: startDate={startDate}, endDate={endDate}")
    print(f"Fechas calculadas: startDate={startDate}, endDate={endDate}")

    return startDate, endDate

@task(log_prints=True, cache_key_fn=lambda *args, **kwargs: None)
async def getConfiguration(mode):
    logger.info(f"Conectando a la base de datos para obtener configuraciones en modo {mode}...")
    print(f"Conectando a la base de datos para obtener configuraciones en modo {mode}...")

    configurations = []
    connStr = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={db_server};DATABASE={db_database};Trusted_Connection=yes;"

    queryFilePaths = {
        "online": '../workload-goanalytics/query_simpleReporting_online.sql',
        "offline": '../workload-goanalytics/query_simpleReporting_offline.sql',
        "cutDay": '../workload-goanalytics/query_simpleReporting_online.sql',
    }

    if mode not in queryFilePaths:
        logger.error(f"Modo inválido: {mode}")
        print(f"Modo inválido: {mode}")
        return []

    sqlQueryPath = queryFilePaths[mode]

    try:
        with open(sqlQueryPath, 'r', encoding='utf-8') as file:
            query = file.read()

        async with aioodbc.connect(dsn=connStr) as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(query)
                rows = await cursor.fetchall()

                startDate, endDate = calculateDates(mode)
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
async def getApi(session, endpoint, token, config):
    logger.info(f"Descargando datos de timeline para {config['campaing']} ({config['userDomain']})...")
    print(f"Descargando datos de timeline para {config['campaing']} ({config['userDomain']})...")
    headers = {"Authorization": f"Bearer {token}"}
    url = f"{endpoint}by_qhour/{config['startDate']}/{config['endDate']}"
    params = {"by": "agent,campaign,direction"}
    try:
        async with session.get(url, headers=headers, params=params) as response:
            response.raise_for_status()
            data = await response.json()

            allDataFrame = []
            for record in data:
                if "by_agent" in record and isinstance(record["by_agent"], list):
                    for agentData in record["by_agent"]:
                        agentData["day_stamp"] = record.get("stamp")
                        agentData["campaing"] = config["campaing"]
                        ##agentData["domaingService"] = config["domaingService"]
                        allDataFrame.append(agentData)
            return transformDate(allDataFrame)
    except Exception as e:
        logger.error(f"Error al descargar datos para {config['campaing']}: {e}")
        print(f"Error al descargar datos para {config['campaing']}: {e}")
        return []

@flow(log_prints=True)
async def workFlow_ApiTimeLineCallsGA(mode):
    logger.info(header)
    print(header)
    logger.info(f"Modo de ejecución: {mode}")
    print(f"Modo de ejecución: {mode}")

    configurations = await getConfiguration(mode)
    if not configurations:
        logger.error("No se encontraron configuraciones.")
        print("No se encontraron configuraciones.")
        return

    allDataTarget = []
    allOriginalData = []

    async with aiohttp.ClientSession() as session:
        tokenTask = [getToken(session, config) for config in configurations]
        tokens = await asyncio.gather(*tokenTask)

        for tokenInfo, config in zip(tokens, configurations):
            if tokenInfo and tokenInfo.get("token"):
                data = await getApi(session, config["epTimeLine"], tokenInfo["token"], config)
                allDataTarget.extend(data)
                allOriginalData.extend(data)  # Guardar los datos originales

    # Crear las carpetas si no existen
    os.makedirs(originalDataFolder, exist_ok=True)
    os.makedirs(baseFolderDE, exist_ok=True)
    os.makedirs(baseFolder, exist_ok=True)

    if allDataTarget:
        df = pd.DataFrame(allDataTarget)
        
        # Guardar el archivo en baseFolderDE con la fecha en el nombre
        output_file_de = os.path.join(baseFolderDE, f"callTimeLine_{currentDate}.txt")
        df.to_csv(output_file_de, index=False, sep="|")
        logger.info(f"Datos consolidados guardados en '{output_file_de}'")
        print(f"Datos consolidados guardados en '{output_file_de}'")

        # Guardar el archivo en baseFolder con el nombre callTimeLine.txt
        output_file_base = os.path.join(baseFolder, "callTimeLine.txt")
        df.to_csv(output_file_base, index=False, sep="|")
        logger.info(f"Datos consolidados guardados en '{output_file_base}'")
        print(f"Datos consolidados guardados en '{output_file_base}'")

    if allOriginalData:
        original_output_file = os.path.join(originalDataFolder, f"callTimeLine_{currentDate}.json")
        with open(original_output_file, 'w', encoding='utf-8') as f:
            json.dump(allOriginalData, f, ensure_ascii=False, indent=4)
        logger.info(f"Datos originales guardados en '{original_output_file}'")
        print(f"Datos originales guardados en '{original_output_file}'")

    query = '''
    SELECT dboName, nameFile , typeFile, delimiter, chunkSize, sp_proccess, db_database, db_server, if_exists, campaing, filePath
    FROM tbl_configuration_workloads_bi WHERE id = 7
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
    asyncio.run(workFlow_ApiTimeLineCallsGA("offline"))
