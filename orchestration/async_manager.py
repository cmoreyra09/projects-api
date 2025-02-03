import sys
import aioodbc
import asyncio
import pandas as pd
from datetime import datetime
import os
from dotenv import load_dotenv
from sqlalchemy import text

sys.path.insert(0, r'../db_connection/')
sys.path.insert(0, r'../utils/')

from connectiondb import sqlConnectDB
from logger import setup_logger

load_dotenv()

currentDate = datetime.now()
logFormatDate = currentDate.strftime("%Y%m%d")

def setupLoggerTask(serviceName, logCfg):
    return setup_logger(log_file=f"../../logs/{serviceName}/{serviceName}_{logFormatDate}.log", logger_name=serviceName)

async def inspecTable(df, table_name, if_exists, db_server, db_name, logger):
    conn_str = f"Driver={{SQL Server}};Server={db_server};Database={db_name};Trusted_Connection=yes;"
    logger.info(f"Iniciando inspección de la tabla '{table_name}' en la base de datos '{db_name}'...")
    print(f"Iniciando inspección de la tabla '{table_name}' en la base de datos '{db_name}'...")
    async with aioodbc.connect(dsn=conn_str) as conn:
        async with conn.cursor() as cursor:
            await cursor.execute(f"SELECT OBJECT_ID('{table_name}', 'U')")
            row = await cursor.fetchone()
            exists = row is not None and row[0] is not None

            if exists:
                if if_exists == 'drop':
                    await cursor.execute(f"DROP TABLE {table_name}")
                    await conn.commit()
                    logger.info(f"Tabla {table_name} eliminada (DROP).")
                    print(f"Tabla {table_name} eliminada (DROP).")
                elif if_exists == 'truncate':
                    await cursor.execute(f"TRUNCATE TABLE {table_name}")
                    await conn.commit()
                    logger.info(f"Tabla {table_name} limpiada (TRUNCATE).")
                    print(f"Tabla {table_name} limpiada (TRUNCATE).")
                elif if_exists == 'append':
                    logger.info(f"Tabla {table_name} existe y se agregarán datos (APPEND).")
                    print(f"Tabla {table_name} existe y se agregarán datos (APPEND).")
                    return  
                else:
                    error_msg = f"Opción if_exists inválida: {if_exists}. Use 'drop', 'truncate', o 'append'."
                    logger.error(error_msg)
                    print(error_msg)
                    raise ValueError(error_msg)

            if not exists or if_exists == 'drop':
                columns = [f"[{col_name}] NVARCHAR(MAX)" for col_name in df.columns]
                columns.extend([
                    "[FechaEjecucionInsert] DATETIME DEFAULT GETDATE()",
                    "[UsuarioEjecucionInsert] NVARCHAR(255) DEFAULT SYSTEM_USER",
                    "[PcEjecucionInsert] NVARCHAR(255) DEFAULT HOST_NAME()"
                ])
                create_sql = f"CREATE TABLE {table_name} ({', '.join(columns)})"
                await cursor.execute(create_sql)
                await conn.commit()
                logger.info(f"Tabla {table_name} creada exitosamente.")
                print(f"Tabla {table_name} creada exitosamente.")

async def ingestDataTask(df, table_name, chunk_size, db_server, db_name, logger):
    df = df.astype(str).replace(['nan', 'NaT'], None)

    if df.empty:
        logger.warning(f"Archivo vacío, sin datos a insertar en {table_name}.")
        print(f"Archivo vacío, sin datos a insertar en {table_name}.")
        return 0

    conn_str = f"Driver={{SQL Server}};Server={db_server};Database={db_name};Trusted_Connection=yes;"
    total_inserted = 0
    num_chunks = (len(df) // chunk_size) + (1 if len(df) % chunk_size != 0 else 0)

    logger.info(f"Iniciando inserción de datos en la tabla '{table_name}'...")
    print(f"Iniciando inserción de datos en la tabla '{table_name}'...")
    async with aioodbc.connect(dsn=conn_str) as conn:
        async with conn.cursor() as cursor:
            insert_query = f"INSERT INTO {table_name} ({', '.join([f'[{col}]' for col in df.columns])}, FechaEjecucionInsert, UsuarioEjecucionInsert, PcEjecucionInsert) VALUES ({', '.join(['?' for _ in df.columns])}, GETDATE(), SYSTEM_USER, HOST_NAME())"

            for i in range(num_chunks):
                chunk_df = df.iloc[i * chunk_size:(i + 1) * chunk_size]
                data_tuples = list(chunk_df.itertuples(index=False, name=None))
                await cursor.executemany(insert_query, data_tuples)
                await conn.commit()

                total_inserted += len(chunk_df)
                logger.info(f"Insertado chunk {i + 1}/{num_chunks} con {len(chunk_df)} filas en la tabla {table_name}.")
                print(f"Insertado chunk {i + 1}/{num_chunks} con {len(chunk_df)} filas en la tabla {table_name}.")

    logger.info(f"Conexión cerrada para la base de datos {db_name}")
    print(f"Conexión cerrada para la base de datos {db_name}")
    return total_inserted

async def spTask(sp_procces, db_name, db_server, logger):
    conn_str = f"Driver={{SQL Server}};Server={db_server};Database={db_name};Trusted_Connection=yes;"
    try:
        logger.info(f"Ejecutando procedimiento almacenado: {sp_procces}")
        print(f"Ejecutando procedimiento almacenado: {sp_procces}")
        async with aioodbc.connect(dsn=conn_str) as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(sp_procces)
                await conn.commit()
                logger.info(f"Procedimiento almacenado ejecutado correctamente en la base de datos '{db_name}'.")
                print(f"Procedimiento almacenado ejecutado correctamente en la base de datos '{db_name}'.")
    except Exception as e:
        logger.error(f"Error al ejecutar el procedimiento almacenado '{sp_procces}': {e}")
        print(f"Error al ejecutar el procedimiento almacenado '{sp_procces}': {e}")

async def fileProccesFlow_wrapper(dbo_name, config, file_inputs, sp_sync_execution, logger):
    try:
        logger.info(f"Iniciando flujo de proceso para '{dbo_name}'...")
        print(f"Iniciando flujo de proceso para '{dbo_name}'...")
        result = await fileProccesFlow(dbo_name, config, file_inputs, sp_sync_execution, logger)
        logger.info(f"Flujo de proceso completado para '{dbo_name}' con {result} filas insertadas.")
        print(f"Flujo de proceso completado para '{dbo_name}' con {result} filas insertadas.")
        return result
    except Exception as e:
        logger.error(f"Error en el flujo {dbo_name}: {e}", exc_info=True)
        print(f"Error en el flujo {dbo_name}: {e}")
        return 0

async def fileProccesFlow(dbo_name, config, file_inputs, sp_sync_execution, logger):
    file_path = os.path.join(file_inputs, f"{config['namefile']}.{config['type_file']}")

    if os.path.exists(file_path):
        logger.info(f"Procesando archivo para '{dbo_name}': {file_path}")
        print(f"Procesando archivo para '{dbo_name}': {file_path}")

        if config['type_file'] == 'csv':
            df = pd.read_csv(file_path, dtype=str, delimiter=config.get('delimiter'))
        elif config['type_file'] == 'xlsx':
            df = pd.read_excel(file_path, dtype=str)
        elif config['type_file'] == 'txt':
            df = pd.read_csv(file_path, dtype=str, delimiter=config.get('delimiter'))
        else:
            logger.warning(f"Tipo de archivo no soportado: {config['type_file']}")
            print(f"Tipo de archivo no soportado: {config['type_file']}")
            return 0

        if df.empty:
            logger.info(f"No hay datos en el archivo '{file_path}' para el proceso '{dbo_name}', el proceso no se ejecutará.")
            print(f"No hay datos en el archivo '{file_path}' para el proceso '{dbo_name}', el proceso no se ejecutará.")
            return 0

        # Obtener el nombre del archivo y su extensión
        file_name, file_extension = os.path.splitext(file_path)
        # Crear el nuevo nombre del archivo con la fecha antes de la extensión
        new_file_name = f"{file_name}_{logFormatDate}{file_extension}"
        # Asignar el nuevo nombre del archivo a la columna 'fileName'

        df['fileName'] = os.path.basename(new_file_name)

        await inspecTable(df, dbo_name, config['if_exists'], config['db_server'], config['db_name'], logger)
        total_inserted = await ingestDataTask(df, dbo_name, config['chunksize'], config['db_server'], config['db_name'], logger)

        if not sp_sync_execution and config['sp_procces']:
            await spTask(config['sp_procces'], config['db_name'], config['db_server'], logger)

        return total_inserted
    else:
        logger.warning(f"Archivo no encontrado para '{dbo_name}': {file_path}")
        print(f"Archivo no encontrado para '{dbo_name}': {file_path}")
        return 0

async def workloadAsync(serviceName, db_config, logCfg, sp_sync_execution, config_rpa):
    logger = setupLoggerTask(serviceName, logCfg)

    logger.info(f"Iniciando proceso de carga para el servicio '{serviceName}'...")
    print(f"Iniciando proceso de carga para el servicio '{serviceName}'...")

    results = await asyncio.gather(*[
        fileProccesFlow_wrapper(dbo_name, config, config['filePath'], sp_sync_execution, logger)
        for dbo_name, config in config_rpa.items()
    ])

    if sp_sync_execution:
        sp_tasks = [
            spTask(config['sp_procces'], config['db_name'], config['db_server'], logger)
            for config in config_rpa.values() if config['sp_procces']
        ]
        await asyncio.gather(*sp_tasks)

    total_inserted_overall = sum(results)
    logger.info(f"Total de filas insertadas en todos los procesos: {total_inserted_overall}")
    print(f"Total de filas insertadas en todos los procesos: {total_inserted_overall}")

def getConfigRpa(query):
    db_server = os.getenv('db_server')
    db_database = os.getenv('db_database')
    db_engine = sqlConnectDB(db_server, db_database)
    configDict = {}

    with db_engine.connectDb().connect() as connection:
        result = connection.execute(text(query))
        for row in result:
            configDict[row._mapping['dboName']] = {
                'namefile': row._mapping['nameFile'],
                'type_file': row._mapping['typeFile'],
                'delimiter': row._mapping['delimiter'],
                'chunksize': row._mapping['chunkSize'],
                'sp_procces': row._mapping['sp_proccess'],
                'db_name': row._mapping['db_database'],
                'db_server': row._mapping['db_server'],
                'if_exists': row._mapping['if_exists'],
                'name_procces': row._mapping['campaing'],
                'filePath': row._mapping['filePath']
            }
    db_engine.closeConnect()
    return configDict

if __name__ == '__main__':
    load_dotenv()
    query = "SELECT dboName, nameFile, typeFile, delimiter, chunkSize, sp_proccess, db_database, db_server, if_exists, campaing, filePath FROM tbl_configuration_workloads_bi"
    settings = getConfigRpa(query)
    first_key = next(iter(settings))
    serviceName = settings[first_key]['name_procces']

    db_config = {
        'server': os.getenv('db_server'),
        'database': os.getenv('db_database')
    }

    asyncio.run(workloadAsync(serviceName=serviceName, db_config=db_config, logCfg={f'../logs/{serviceName}'}, sp_sync_execution=False))

