#!/usr/bin/env bash
set -ex

function tableExists(){
  local db=$1
  local table=$2
  validateTable=$(hive --database $db -e "SHOW TABLES LIKE '$table'")
  if [[ -z $validateTable ]]; then
  #echo "Error:: $table cannot be found"
  return 1
  fi
  return 0
}

function putToHDFS(){
  hdfs dfs -mkdir -p $2
  hdfs dfs -put -f ${DIR}/$1 $2
}

function mkDir(){
  hdfs dfs -mkdir -p $1
}

function generateHiveVar(){
  rm -rf ${DIR}/task$1/hql/hivevar.sql;
  touch ${DIR}/task$1/hql/hivevar.sql;
  echo "set hive.variable.substitute=true;" >> ${DIR}/task$1/hql/hivevar.sql;
  echo "set hive.explain.user=true" >> ${DIR}/task$1/hql/hivevar.sql;
  while read line; do
      echo "set hivevar:$line;" >> ${DIR}/task$1/hql/hivevar.sql;
  done < ${ENV}.properties
}

function prop() {
    grep -w "${1}=.*" ${ENV}.properties|cut -d'=' -f2
    #grep "${1}" ${ENV}.properties|cut -d'=' -f2
}

function runHive(){
   hive -v --hiveconf dir=${DIR} -f ${DIR}/$1
}
