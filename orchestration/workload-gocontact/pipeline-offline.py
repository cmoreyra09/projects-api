import sys
import aioodbc
import asyncio
import pandas as pd
from datetime import datetime
import os
from dotenv import load_dotenv
from sqlalchemy import text
from prefect import task, flow
from collections import defaultdict

sys.path.insert(0, r'../../db_configuration/')
sys.path.insert(0, r'../../utils/')
sys.path.insert(0, r'../workload-gocontact/')

from getApiCfg import workflowGetApiGoContact
from connectiondb import Connection_SQL_Server_Async
from logger import setup_logger
from connectiondb import sqlConnectDB

load_dotenv()

##@task
def setupLoggerTask(serviceName, logConfig):
    currentDate = datetime.now()
    logDate = currentDate.strftime("%Y%m%d")
    return setup_logger(log_file=f"../../logs/api_gocontact/{logConfig['logPath']}/{serviceName}_{logDate}.log", logger_name=serviceName)

@task(log_prints=True)
def getConfigOnline(logger):

    dbServer = os.getenv('db_server')
    dbDatabase = os.getenv('db_database')
    dbEngine = sqlConnectDB(dbServer, dbDatabase)
    
    configDict = {}
    
    with dbEngine.connectDb().connect() as connection:
        query = '''
            SELECT dboName, nameFile, typeFile, delimiter, chunkSize, sp_proccess, db_database, db_server, if_exists, blockTarget
            FROM tbl_configuration_gocontact WHERE stateReg = 1 
        '''
        result = connection.execute(text(query))
        
        for row in result:
            configDict[row[0]] = {
                'nameFile': row[1],
                'typeFile': row[2],
                'delimiter': row[3],
                'chunkSize': row[4],
                'sp_proccess': row[5],  
                'dbName': row[6],
                'dbServer': row[7],
                'ifExists': row[8],
                'blockTarget': row[9]
            }
            
    dbEngine.closeConnect()
    logger.info('Se recopiló la información de la configuración online.')
    print('Se recopiló la información de la configuración online.')
    
    return configDict

@task(log_prints=True)
async def createDboTask(df, tableName, dbServer, dbName, logger):
    if df.empty:
        logger.warning(f'DataFile vacío, no se procederá con la creación de la tabla: {tableName}')
        print(f'DataFile vacío, no se procederá con la creación de la tabla: {tableName}')
        return 0
    
    connStr = f"Driver={{SQL Server}};Server={dbServer};Database={dbName};Trusted_Connection=yes;"
    async with aioodbc.connect(dsn=connStr) as conn:
        async with conn.cursor() as cursor:
            columns = [f"[{col}] NVARCHAR(MAX)" for col in df.columns]
            columns.extend([
                "[FechaEjecucionInsert] DATETIME DEFAULT GETDATE()",
                "[UsuarioEjecucionInsert] NVARCHAR(255) DEFAULT SYSTEM_USER",
                "[PcEjecucionInsert] NVARCHAR(255) DEFAULT HOST_NAME()"
            ])
            createSql = f"CREATE TABLE {tableName} ({', '.join(columns)})"
            await cursor.execute(createSql)
            await conn.commit()
            logger.info(f"Tabla {tableName} creada exitosamente.")
            print(f"Tabla {tableName} creada exitosamente.")

@task(log_prints=True)
async def ingestDataTask(df, tableName, chunkSize, ifExists, dbServer, dbName, logger):
    
    if df.empty:
        logger.warning(f"Archivo vacío, sin datos a insertar en {tableName}.")
        print(f"Archivo vacío, sin datos a insertar en {tableName}.")
        return 0
    
    df = df.astype(str).replace(['nan', 'NaT'], None)
    connStr = f"Driver={{SQL Server}};Server={dbServer};Database={dbName};Trusted_Connection=yes;"
    totalInserted = 0
    numChunks = (len(df) // chunkSize) + (1 if len(df) % chunkSize != 0 else 0)

    async with aioodbc.connect(dsn=connStr) as conn:
        async with conn.cursor() as cursor:
            await cursor.execute(f"SELECT OBJECT_ID('{tableName}', 'U')")
            row = await cursor.fetchone()
            exists = row is not None and row[0] is not None
            
            if not exists:
                await createDboTask(df, tableName, dbServer, dbName, logger)
            elif ifExists == 'drop':
                await cursor.execute(f"DROP TABLE {tableName}")
                await conn.commit()
                await createDboTask(df, tableName, dbServer, dbName, logger)
            elif ifExists == 'truncate':
                await cursor.execute(f"TRUNCATE TABLE {tableName}")
                await conn.commit()
            
            insertQuery = f"INSERT INTO {tableName} ({', '.join([f'[{col}]' for col in df.columns])}, FechaEjecucionInsert, UsuarioEjecucionInsert, PcEjecucionInsert) VALUES ({', '.join(['?' for _ in df.columns])}, GETDATE(), SYSTEM_USER, HOST_NAME())"
            
            for i in range(numChunks):
                chunkDataFrame = df.iloc[i * chunkSize:(i + 1) * chunkSize]
                dataTuples = list(chunkDataFrame.itertuples(index=False, name=None))
                await cursor.executemany(insertQuery, dataTuples)
                await conn.commit()
                
                totalInserted += len(chunkDataFrame)
                logger.info(f"Insertado chunk {i + 1}/{numChunks} con {len(chunkDataFrame)} filas en la tabla {tableName}.")
                print(f"Insertado chunk {i + 1}/{numChunks} con {len(chunkDataFrame)} filas en la tabla {tableName}.")

    logger.info(f"Conexión cerrada para la base de datos {dbName}")
    print(f"Conexión cerrada para la base de datos {dbName}")
    return totalInserted

@task(log_prints=True)
async def executeSpTask(sp_proccess, dbName, dbServer, logger):
    connStr = f"Driver={{SQL Server}};Server={dbServer};Database={dbName};Trusted_Connection=yes;"
    try:
        async with aioodbc.connect(dsn=connStr) as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(sp_proccess)
                await conn.commit()
                logger.info(f"Procedimiento almacenado ejecutado correctamente en la base de datos '{dbName}'.")
                print(f'Procedimiento almacenado ejecutado correctamente en la base de datos {dbName}')
    except Exception as e:
        logger.error(f"Error al ejecutar el procedimiento almacenado : {e}")
        print(f"Error al ejecutar el procedimiento almacenado : {e}")

@task(log_prints=True)
async def executeSpBlock(spList, dbName, dbServer, logger, blockTarget):
    logger.info(f"Iniciando ejecución del bloque '{blockTarget}' con {len(spList)} procedimientos almacenados.")
    print(f"Iniciando ejecución del bloque '{blockTarget}' con {len(spList)} procedimientos almacenados.")

    for sp in spList:
        logger.info(f"Ejecutando procedimiento almacenado en el bloque '{blockTarget}' de forma sincrónica.")
        print(f"Ejecutando procedimiento almacenado en el bloque '{blockTarget}' de forma sincrónica.")
        await executeSpTask(sp, dbName, dbServer, logger)

    logger.info(f"Ejecución del bloque '{blockTarget}' completada.")
    print(f"Ejecución del bloque '{blockTarget}' completada.")

@task(log_prints=True)
def format_column_task(df):
    df.columns = [
        col.translate(str.maketrans('', '', '!"#$%&\'()*+,./:;<=>?@[\\]^_`{|}~')).strip()[:100] for col in df.columns
    ]
    return df

@task(log_prints=True)
async def processFileTask(dboName, config, fileInputs, spSyncExecution, logger):
    filePath = os.path.join(fileInputs, f"{config['nameFile']}{datetime.now().strftime('%Y%m%d')}.{config['typeFile']}")
    if os.path.exists(filePath):
        logger.info(f"Procesando archivo para '{dboName}': {filePath}")
        df = pd.read_csv(filePath, dtype=str, delimiter=config.get('delimiter'))
        
        if df.empty:
            logger.info(f"No hay datos en el archivo '{filePath}' para el proceso '{dboName}', el proceso no se ejecutará.")
            print(f"No hay datos en el archivo '{filePath}' para el proceso '{dboName}', el proceso no se ejecutará.")
            return 0  
        
        df = format_column_task(df)
        
        
        df['fileName'] = os.path.basename(filePath) ## Columna para guardar el nombre del archivo

        
        totalInserted = await ingestDataTask(df, dboName, config['chunkSize'], config['ifExists'], config['dbServer'], config['dbName'], logger)
        
        if not spSyncExecution and config['sp_proccess']:
            await executeSpTask(config['sp_proccess'], config['dbName'], config['dbServer'], logger)
        
        return totalInserted
    else:
        logger.warning(f"Archivo no encontrado para '{dboName}': {filePath}")
        return 0

@flow(log_prints=True)
async def workFlow_ApiGoContactOfline(serviceName, dbConfig, logConfig, spSyncExecution):
    
    
    await workflowGetApiGoContact(online=False)
    
    logger = setupLoggerTask(serviceName, logConfig)
    configRpa = getConfigOnline(logger)
    fileInputs = r'../../data/03_TARGETDATA/data_api_gocontact/'
    
    print(f"Iniciando Proceso Ingesta para workflow online")
    logger.info(f"Iniciando Proceso Ingesta para workflow online")

    blockGroups = defaultdict(list)
    for dboName, config in configRpa.items():
        block = config.get('blockTarget')
        blockGroups[block].append(config)

    fileTasks = [
        asyncio.create_task(
            processFileTask(dboName, config, fileInputs, spSyncExecution, logger)
        )
        for dboName, config in configRpa.items()
    ]
    
    results = await asyncio.gather(*fileTasks)

    if spSyncExecution:
        blockTasks = []
        for block, spConfigs in blockGroups.items():
            spList = [config['sp_proccess'] for config in spConfigs if config['sp_proccess']]
            if spList:
                blockTasks.append(
                    asyncio.create_task(executeSpBlock(spList, dbConfig['database'], dbConfig['server'], logger, block))
                )
        await asyncio.gather(*blockTasks)

    totalInsertedOverall = sum(results)
    print(f"Total de filas insertadas en todos los procesos: {totalInsertedOverall}")
    logger.info(f"Total de filas insertadas en todos los procesos: {totalInsertedOverall}")

# Ejecución
if __name__ == "__main__":
    serviceName = "workflow_gocontact_offline"
    
    dbConfig = {
        'server': os.getenv('db_server'),
        'database': os.getenv('db_database')
    }
    
    logConfig = {
        'logPath': '../../logs/workflow_gocontact/'
    }
    
    asyncio.run(workFlow_ApiGoContactOfline(serviceName, dbConfig, logConfig, spSyncExecution=True))
