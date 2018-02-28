#!/usr/bin/env bash
HDFS_WORKING_DIR=$(prop 'hdfs_data_dir')

function loadDataForTables(){
  putToHDFS ../$(prop 'user_agent_jar') ${HDFS_WORKING_DIR}/
  putToHDFS ../$(prop 'dependency_jar') ${HDFS_WORKING_DIR}/
  putToHDFS /files/city.en.txt ${HDFS_WORKING_DIR}/$(prop 'city_dir')
  putWithRenamingAccordingtoPartition
}

function setupTables(){
  runHive task3/hql/setup.sql
}

function putWithRenamingAccordingtoPartition(){
  for i in ${DIR}/files/imp*; do
     filename=$(basename "$i")
     txt="${filename##*.}"
     filenameWoTXT="${filename%.*}"
     dateF="${filenameWoTXT##*.}"
     dateO=$(date --date="$dateF" "+%Y-%m-%d")
     putToHDFS /files/${filename}/* ${HDFS_WORKING_DIR}/$(prop 'impr_dir')/$(prop 'imp_part_name')=${dateO}
  done
}

function checkRequired(){
  local ret;
  $(tableExists $(prop 'db_name') impression_log);
  ret=$?;
  if [[ "$ret" != "0" ]];
    then
      return ${ret};
    else
      $(tableExists $(prop 'db_name') city);
      ret=$?;
      return ${ret};
  fi
  return 0;
}
