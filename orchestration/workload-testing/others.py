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
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from async_manager import workloadAsync, getConfigRpa

if __name__ == '__main__':
    load_dotenv()


    sqlQuery = f'''
        SELECT dboName, nameFile , typeFile, delimiter, chunkSize, sp_proccess, db_database, db_server, if_exists, campaing, filePath FROM tbl_configuration_workloads_bi WHERE Id = 14
        '''

    settings = getConfigRpa(sqlQuery)
    first_key = next(iter(settings))
    serviceName = settings[first_key]['name_procces']

    db_config = {
        'server': os.getenv('db_server'),
        'database': os.getenv('db_database')
    }

    asyncio.run(workloadAsync(
        serviceName=serviceName,
        db_config=db_config,
        logCfg={f'../../logs/{serviceName}'},
        sp_sync_execution=False,
        config_rpa=settings
    ))


