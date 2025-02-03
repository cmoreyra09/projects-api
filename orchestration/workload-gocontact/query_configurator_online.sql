
-- Query configurator si se precisa agregar o reprocesar un api en especifico hacerle el where 

    SELECT 
        url + api AS url,
        action,
        domain,
        userName,
        password,
        apiDownload,
        ownerType,
        ownerId,
        dataType,
        templateId,
        includeAllowners,
        dateIni = NULL,
        dateEnd = NULL,
        nameFile,
        dboName
    FROM tbl_configuration_gocontact
    WHERE flatInterval = 1 and stateReg = 1 

