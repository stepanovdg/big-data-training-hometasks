#!/usr/bin/env bash


function runChild(){
  runMysql "TRUNCATE TABLE ${SQOOP_EXPORT_STAGING_TABLE}";
## Mappers number = (12 partitions = 12) * (300 mb zize of partitions = 3) = 36
## Depends on cluster hardware could be decreased, but not in our case.
## Also checked at database side - it has required number of connectors.


## Default is 4 mappers.
  sqoop export --connect $(prop 'mysql_url') --table ${SQOOP_EXPORT_TABLE} --options-file ${CRED_FILE}\
   --export-dir ${SQOOP_EXPORT_DIR} -m $(prop 'mappers') --input-fields-terminated-by ',' --lines-terminated-by '\n'\
    --staging-table ${SQOOP_EXPORT_STAGING_TABLE} --clear-staging-table --columns stationId,date,tmin,tmax,snow,snwd,prcp\
     --direct;

  runMysql "SELECT count(*) FROM ${SQOOP_EXPORT_STAGING_TABLE};";
  runMysql "SELECT count(*) FROM ${SQOOP_EXPORT_DIR};";
  runMysql "SELECT * FROM ${SQOOP_EXPORT_DIR} ORDER BY stationid, date LIMIT 10;";

}