#!/usr/bin/env bash
HDFS_WORKING_DIR=$(prop 'hdfs_data_dir')

function loadDataForTables(){
  putToHDFS /files/airports.csv $HDFS_WORKING_DIR/$(prop 'airports_dir')
  putToHDFS /files/carriers.csv $HDFS_WORKING_DIR/$(prop 'carriers_dir')
  putToHDFS /files/2007.csv.bz2 $HDFS_WORKING_DIR/$(prop 'flights_dir')
}

function setupTables(){
  runHive task2/hql/setup.sql
}

function checkRequired(){
  local ret;
  $(tableExists $(prop 'db_name') flights);
  ret=$?;
  if [[ "$ret" != "0" ]];
    then
      return ${ret};
    else
      $(tableExists $(prop 'db_name') carriers);
      ret=$?;
      if [[ "$ret" != "0" ]];
        then
         return ${ret};
        else
          $(tableExists $(prop 'db_name') airports);
          ret=$?;
          return ${ret};
      fi
  fi
  return 0;
#  return $(tableExists $(prop 'db_name') airports) &&
#    $(tableExists $(prop 'db_name') carriers) &&
#    $(tableExists $(prop 'db_name') flights);
}
