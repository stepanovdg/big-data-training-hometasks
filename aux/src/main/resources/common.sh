#!/usr/bin/env bash

function tableExists(){
  local table=$1
  local validateTable=$(runMysql "SHOW TABLES LIKE '$table'")
  if [[ -z $validateTable ]]; then
  #echo "Error:: $table cannot be found"
  return 1
  fi
  return 0
}

function putToHDFS(){
  hdfs dfs -mkdir -p $2
  hdfs dfs -put -f ${DIR}/$1 $2
  hdfs dfs -chmod -R 777 $2
}

function mkDir(){
  hdfs dfs -mkdir -p $1
}

function prop() {
  grep -w "${1}=.*" ${ENV}.properties|cut -d'=' -f2
}

function runMysql(){
  mysql -u $(prop 'mysql_user') -password=$(prop 'mysql_password') $1 \q
}