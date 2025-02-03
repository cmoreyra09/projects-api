SELECT
    DISTINCT 
    userDomain,
    passColumn,
    campaing,
    SUBSTRING(userDomain, CHARINDEX('@', userDomain), LEN(userDomain)) AS domaingService,
    url + '/poll/auth/token' AS urlAuth,
    url + '/poll/api/goanalytics/timeline/calls/' AS epTimeLine
FROM tbl_configuration_gocontact
WHERE userDomain IS NOT NULL;