#!/usr/bin/env bash


function runChild(){
  runMysql "TRUNCATE TABLE ${SQOOP_EXPORT_STAGING_TABLE}";
## Mappers number = (12 partitions = 12) * (300 mb zize of partitions = 2) = 24
## As total splits are 25 - i think 25 mappers would the best size in case we have enough hardware power
## Depends on cluster hardware could be decreased as in our case.
## For me better to set 3 mappers as currently internal mysql database is a bottleneck.
## Default is 4 mappers. But I got cluster hanging with  4 mappers and failing the same job after 3,411.7924 seconds
## One mapper also work - but job works longer.

  sqoop export --connect $(prop 'mysql_url') --table ${SQOOP_EXPORT_TABLE} --options-file ${CRED_FILE}\
   --export-dir ${SQOOP_EXPORT_DIR} -m $(prop 'mappers') --input-fields-terminated-by ',' --lines-terminated-by '\n'\
    --staging-table ${SQOOP_EXPORT_STAGING_TABLE} --clear-staging-table --columns stationId,date,tmin,tmax,snow,snwd,prcp\
     ;

  runMysql "SELECT count(*) FROM ${SQOOP_EXPORT_STAGING_TABLE};";
  runMysql "SELECT count(*) FROM ${SQOOP_EXPORT_TABLE};";
  runMysql "SELECT * FROM ${SQOOP_EXPORT_TABLE} ORDER BY stationid, date LIMIT 10;";

}