import sys
sys.path.insert(0, r'../../db_configuration/')
sys.path.insert(0, r'../../utils/')

from connectiondb       import Connection_SQL_Server_Async
from logger             import setup_logger

from datetime           import datetime , timedelta
from dotenv             import load_dotenv

import asyncio
import os
import time
import aiohttp
import pandas
import pyfiglet

##from prefect import task, flow

load_dotenv()


baseFolder                      =               r'../../data/03_TARGETDATA/data_api_gocontact/'  # FilePath downloads

queryExecute                    =               r'../../inputs_api/gocontact/query_configurator.sql'  # QueryConfiguration Select specify

currentDate                     =               datetime.now().strftime("%Y%m%d")

timeExecuting                   =               datetime.now().strftime("%Y-%m-%d %H:%M:%S")

banner                          =               pyfiglet.figlet_format(f"WORKFLOW API GO CONTACT")

header                          =               f"{'=' * 50}\n{banner}{'=' * 10}\nIniciando workflow ApiGoContact - {timeExecuting}"




def setupLoggerTask(mode):
    
    currentDate = datetime.now().strftime("%Y%m%d")
    logFileName = f'../../logs/workflow_gocontact/workflow_gocontact_{mode}_{currentDate}.log'
    
    return setup_logger(log_file=logFileName, logger_name=f'ApiGoContact_{mode}')



##@task(log_prints=True)
async def executeQuery( conn , query  =  str ):
    

    try:
    
        async with conn.cursor() as cursor:
            
            await cursor.execute(query)
            
            result           =               await cursor.fetchone()
            
            return result[0] if result else None
    
    except Exception as e:
        
        logger.error(f"Error al ejecutar consulta de fecha: {e}")
        print(f"Error al ejecutar consulta de fecha: {e}")
        
        return None


##@task(log_prints=True)
async def getdboConfiguration(online=None):
    
    if online == True:
        mode = "online"
        
    elif online == False:
        
        mode = "offline"
    
    elif online == "cutDay":
    
        mode = "cutDay"
    
    else:
        mode = "default"

    global logger
    logger = setupLoggerTask(mode)

    if online is None:
        logger.warning("No se ha especificado el modo de configuración. Establezca `online=True`, `online=False`, o `online='cutDay'`.")
        print("No se ha especificado el modo de configuración. Establezca `online=True`, `online=False`, o `online='cutDay'`.")
        return []

    tblconfigDbolist = []  

    connectAsync = Connection_SQL_Server_Async(os.getenv('db_server'), os.getenv('db_database'))
    connection = await connectAsync.connect_db()

    if connection is None:
        logger.error("Error al conectar a la base de datos.")
        print("Error al conectar a la base de datos.")
        return tblconfigDbolist  


    if online == True:
        queryFile = r'../workload-gocontact/query_configurator_online.sql'
        
        logger.info(header)
        print(header)
        
        logger.info(f'Ejecutando proceso online. {timeExecuting}, fileQuery ejecutado : {queryFile}')
        print(f'Ejecutando proceso online. {timeExecuting}, fileQuery ejecutado : {queryFile}')
    
    elif online == False:
        queryFile = r'../workload-gocontact/query_configurator_ofline.sql'
        
        logger.info(header)
        print(header)
        
        logger.info(f'Ejecutando proceso offline. {timeExecuting}, fileQuery ejecutado : {queryFile}')
        print(f'Ejecutando proceso offline., fileQuery ejecutado : {queryFile}')
        
    elif online == 'cutDay':
        queryFile = r'../workload-gocontact/query_configurator_online.sql'
        
        logger.info(header)
        print(header)
        
        logger.info(f'Ejecutando proceso cutDay. {timeExecuting}, filequery ejecutado: {queryFile}')
        print(f'Ejecutando proceso cutDay, fileQuery ejecutado : {queryFile}')
    
    else:
        logger.error('Opción inválida, coloque el parámetro correcto')
        print('Opción inválida, coloque el parámetro correcto')
        return []

    try:
        async with connection.cursor() as cursor:
            with open(queryFile, 'r') as file:  
                consulta_sql = file.read()

            try:
                await cursor.execute(consulta_sql)
                rows = await cursor.fetchall()
            except Exception as e:
                logger.error(f"Error al ejecutar la consulta SQL en el archivo {queryFile}: {e}")
                print(f"Error al ejecutar la consulta SQL en el archivo {queryFile}: {e}")
                return []

            for row in rows:
                if online == True:
                
                    currentDateCfg = datetime.now()
                    startDate = (currentDateCfg - timedelta(hours=1)).strftime("%Y-%m-%d %H:%M:%S")
                    endDate = currentDateCfg.strftime("%Y-%m-%d %H:%M:%S")
                    
                    print(f"startDate: {startDate} endDate: {endDate}")
                    logger.info(f"startDate: {startDate} endDate: {endDate}")
                
                elif online == False:
                    
                    currentToday        = datetime.now()
                    currentDateQuery    = datetime.now() - timedelta(days=1)
                    startDate           = currentDateQuery.replace(hour=0, minute=0, second=0, microsecond=0).strftime("%Y-%m-%d %H:%M:%S")
                    ##startDate = currentToday.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0).strftime("%Y-%m-%d %H:%M:%S")
                    endDate             = currentToday.replace(hour=23, minute=59, second=0, microsecond=0).strftime("%Y-%m-%d %H:%M:%S")              
                    
                    ## queryDateini = row[11]
                    ## queryDateend = row[12]
                    ## startDate = await executeQuery(connection, queryDateini)
                    ## endDate = await executeQuery(connection, queryDateend)
                    
                    print(f"startDate: {startDate} endDate: {endDate}")
                    logger.info(f"startDate: {startDate} endDate: {endDate}")
                    
                elif online == 'cutDay':
                    
                    currentDateToday = datetime.now()
                    startDate = currentDateToday.replace(hour=0, minute=0, second=0, microsecond=0).strftime("%Y-%m-%d %H:%M:%S")
                    endDate = currentDateToday.replace(hour=23, minute=59, second=59, microsecond=0).strftime("%Y-%m-%d %H:%M:%S")
                    
                    print(f"startDate: {startDate} endDate: {endDate}")
                    logger.info(f"startDate: {startDate} endDate: {endDate}")
                    
                else:
                    
                    logger.error('Opción inválida en la iteración.')
                    print('Opción inválida en la iteración.')
                    continue

                tblconfigDbolist.append({
                    
                    "url"               : row[0],
                    
                    "action"            : row[1],
                    
                    "domain"            : row[2],
                    
                    "userName"          : row[3],
                    
                    "password"          : row[4],
                    
                    "api_download"      : row[5],
                    
                    "ownerType"         : row[6],
                    
                    "ownerId"           : row[7],
                    
                    "dataType"          : row[8],
                    
                    "templateId"        : row[9],
                    
                    "includeAllowners"  : row[10],
                    
                    "startDate"         : startDate,
                    
                    "endDate"           : endDate,
                    
                    "nameFile"          : row[13],
                    
                    "dboName"           : row[14]
                
                })

        logger.info("[tblconfigDbolist] obtenidas exitosamente.")
        print("[tblconfigDbolist] obtenidas exitosamente.")
        
    except Exception as e:
        
        logger.error(f"Error al obtener [tblconfigDbolist] de la base de datos: {e}")
        print(f"Error al obtener [tblconfigDbolist] de la base de datos: {e}")
        
    finally:
        await connectAsync.close_connection()

    return tblconfigDbolist



##@task(log_prints=True)
async def executeGetapi(session, config):
    
    
    startTime                              =               time.time()
    
    messageRequest                         =               f"Enviando solicitud para {config['nameFile']} con usuario {config['userName']} a {config['url']} de la tabla {config['dboName']}"
    
    logger.info(messageRequest)
    print(messageRequest)

    payload = {
        
        "action"                :   config["action"],
        
        "domain"                :   config["domain"],
        
        "username"              :   config["userName"],
        
        "password"              :   config["password"],
        
        "api_download"          :   config["api_download"],
        
        "ownerType"             :   config["ownerType"],
        
        "ownerId[]"             :   config["ownerId"], # In the case of using IVR, avoid using IN with IDs within brackets ([]), as it is not enabled.
        
        "startDate"             :   config["startDate"],
        
        "endDate"               :   config["endDate"],
        
        "dataType"              :   config["dataType"],
        
        "templateId"            :   config["templateId"],
        
        "includeALLOwners"      :   config["includeAllowners"]
    
    }

    try:
    
        async with session.post(config["url"], data=payload) as response:
            
            response.raise_for_status()
        
            reportName                     =           await response.text()
            reportName                     =           reportName.strip().replace("\"", "")

            if reportName == "Invalid Credentials":
                
                logger.error(f"Credenciales inválidas para {config['userName']} en {config['nameFile']} de la tabla {config['dboName']}" )
                print(f"Credenciales inválidas para {config['userName']} en {config['nameFile']} de la tabla {config['dboName']}" )

                return

            logger.info(f"Solicitud de creación exitosa. Nombre del archivo generado: {reportName} para la tabla {config['dboName']}")
            print(f"Solicitud de creación exitosa. Nombre del archivo generado: {reportName} para la tabla {config['dboName']}")

    except aiohttp.ClientResponseError as e:
        
        logger.error(f"Error en la creación del reporte para {config['nameFile']}: {e.status} - {e.message} de la tabla {config['dboName']}")
        print(f"Error en la creación del reporte para {config['nameFile']}: {e.status} - {e.message} de la tabla {config['dboName']}")
        
        return

    # ExportReporting
    
    downloadParams                 =   {
        
        "action"                    : "getCsvReportFile",
        
        "domain"                    : config["domain"],
        
        "username"                  : config["userName"],
        
        "password"                  : config["password"],
        
        "api_download"              : config["api_download"],
        
        "file"                      : reportName
    }


    downloadUrl                     = config["url"] # Endpoint
    
    try:
    
        async with session.get(downloadUrl, params=downloadParams) as downloadResponse:
    
            downloadResponse.raise_for_status()
    
            if downloadResponse.status == 200:
    

                fileName                   =       f"{config['nameFile']}{datetime.now().strftime('%Y%m%d')}.csv" ## Method to export datafile by date asci example = 20241025
                
                filePath                   =       os.path.join(baseFolder, fileName)
    
                with open(filePath, "wb") as file:
                    file.write(await downloadResponse.read())
                
                logger.info(f"Archivo descargado y guardado como {filePath}")
                print(f"Archivo descargado y guardado como {filePath}")
    
            else:
                
                error_msg                  =        f"Error al descargar el archivo: {downloadResponse.status} - {await downloadResponse.text()}"
    
                logger.error(error_msg)
                print(error_msg)
    
    except aiohttp.ClientResponseError as e:
        
        error_msg = f"Error al descargar el reporte para {config['nameFile']}: {e.status} - {e.message} de la tabla {config['dboName']}"
        
        logger.error(error_msg)
        print(error_msg)
    
    except Exception as e:
        error_msg = f"Error inesperado al descargar el reporte para {config['nameFile']} de la tabla {config['dboName']}: {e}"

        logger.error(error_msg)
        print(error_msg)
    
    elapsed_time = time.time() - startTime
    
    logger.info(f"Tiempo de ejecución para {config['nameFile']}: {elapsed_time:.2f} segundos ")
    print(f"Tiempo de ejecución para {config['nameFile']}: {elapsed_time:.2f} segundos")
    
    
##@flow(name = "workflowGetApiGoContactlog", log_prints=True)
async def workflowGetApiGoContact(online=None):
    
    if online is None:
        logger.warning("No se ha especificado el modo de ejecución. Establezca `online=True` o `online=False`.")
        print("No se ha especificado el modo de ejecución. Establezca `online=True` o `online=False`.")
        return

    tblconfigDbolist = await getdboConfiguration(online=online)
    
    if not tblconfigDbolist:
        
        print("No se encontraron configuraciones en tblconfigDbolist.")
        
        return
    
    async with aiohttp.ClientSession() as session:
        
        tasks = [executeGetapi(session, config) for config in tblconfigDbolist]
        
        await asyncio.gather(*tasks, return_exceptions=True)


if __name__ == "__main__":
    
    #Option True Online , Option False Ofline , Option cutDay for cut day
    
    start_time = time.time()  # Iniciar el temporizador general
    
    asyncio.run(workflowGetApiGoContact(online=False))
    
    total_elapsed_time = time.time() - start_time
    
    formatted_time = time.strftime("%H:%M:%S", time.gmtime(total_elapsed_time))
    
    print(f"Tiempo total de ejecución del proceso: {formatted_time}")

