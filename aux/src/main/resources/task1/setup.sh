#!/usr/bin/env bash

HDFS_WORKING_DIR=$(prop 'hdfs_data_dir')
CRED_FILE=${DIR}/task1/.access_credentials
SQOOP_EXPORT_TABLE=$(prop 'export_table')
SQOOP_EXPORT_STAGING_TABLE=$(prop 'staging_table')
SQOOP_EXPORT_DIR=${HDFS_WORKING_DIR}/weather/

function loadDataForTables(){
  putToHDFS ../heavy/better-format.zip ${SQOOP_EXPORT_DIR}
}

function setupTables(){
  setupAccessFile;
  createExportTable;
  createStagingTable;
}

function checkRequired(){
  local ret;
  $(tableExists ${SQOOP_EXPORT_TABLE});
  ret=$?;
  if [[ "$ret" != "0" ]];
    then
      return ${ret};
    else
      $(tableExists ${SQOOP_EXPORT_STAGING_TABLE});
      ret=$?;
      return ${ret};
  fi
  return 0;
}

function setupAccessFile(){
  touch ${CRED_FILE};
  chmod o+rw ${CRED_FILE};
  echo "--username $(prop 'mysql_user') --password $(prop 'mysql_password')" >> ${CRED_FILE};
}

function createStagingTable(){
  runMysql "create table IF NOT EXISTS ${SQOOP_EXPORT_STAGING_TABLE} as select * from ${SQOOP_EXPORT_TABLE} where 1=2;"
}

function createExportTable(){
  runMysql "create table IF NOT EXISTS ${SQOOP_EXPORT_TABLE} ( stationId varchar(11), date DATE, tmin int, tmax int, snow int, snwd int, prcp int);"
}