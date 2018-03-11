#!/usr/bin/env bash

function loadDataForTables(){
  rm -rf $(prop 'pos_dir');
  mkdir -p $(prop 'pos_dir');
  #touch $(prop 'pos_file');
}

function setupTables(){
  #-Dflume.root.logger=DEBUG,console -Dorg.apache.flume.log.printconfig=true -Dorg.apache.flume.log.rawdata=true
  nohup flume-ng agent -n "$(prop 'agent_name')" -c conf -f ${DIR}/task2/flume-conf.properties  > flume.log &
}

function checkRequired(){
  local ret;
  output=`ps aux|grep $(prop 'agent_name')|grep -v grep`
  set -- $output
  pid=$2
  kill $pid
  sleep 2
  kill -9 $pid >/dev/null 2>&1
  return 1
}