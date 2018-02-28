#!/usr/bin/env bash

function displaySelectHelp(){

  if [[ -n $1 ]];
   then
    echo "Possible selects:
      0 - Use data from you UDF and find most popular device, browser, OS for each city. "
   else
    sel$1help;
  fi
}

function sel0(){
  runHive task3/hql/most_popular_device_browser_OS_for_each_city.sql
}

