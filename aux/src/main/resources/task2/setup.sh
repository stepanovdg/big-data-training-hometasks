#!/usr/bin/env bash

function loadDataForTables(){
  return 0;
}

function setupTables(){
  #-Dflume.root.logger=DEBUG,console -Dorg.apache.flume.log.printconfig=true -Dorg.apache.flume.log.rawdata=true
  bin/flume-ng agent -n $(prop 'agent_name') -c conf -f conf/flume-conf.properties.template
}

function checkRequired(){
  local ret;
  #ps aux | grep $(prop 'agent_name')
  ps -fC $(prop 'agent_name')
  ret=$?;
  if [[ "$ret" != "0" ]];
    then
      return 0;
  fi
  return 1;
}