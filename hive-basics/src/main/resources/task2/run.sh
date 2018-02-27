#!/usr/bin/env bash

function displaySelectHelp(){

  if [[ -n $1 ]];
   then
    echo "Possible selects:
      0 - All carriers who cancelled more than 1 flights during 2007 ,
          order them from biggest to lowest by number
          of cancelled flights and list in each record
           all departure cities where cancellation happened. "
   else
    sel$1help;
  fi
}

function sel0(){
  runHive task2/hql/all_carriers_cancelled_more_than_1_flight_during_2007.sql
}

