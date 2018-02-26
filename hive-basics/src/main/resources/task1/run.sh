#!/usr/bin/env bash

function displaySelectHelp(){

  if [[ -n $1 ]];
   then
    echo "Possible selects:
      0 - Count total number of flights per carrier in 2007
      1 - The total number of flights served in Jun 2007 by NYC
      2 - Find five most busy airports in US during Jun 01 - Aug 31.
      3 - Find the carrier who served the biggest number of flights "
   else
    sel$1help;
  fi
}

function sel0(){
  runHive task1/hql/count_total_number_of_flights_per_carrier_in_2007.sql
}

function sel1(){
  runHive task1/hql/total_number_flights_served_Jun_2007_by_NYC_\(all_airports_use_join_with_Airports\).sql
}

function sel2(){
  runHive task1/hql/five_most_busy_airports_in_US_during_Jun_01_-_Aug_31.sql
}

function sel3(){
  runHive task1/hql/carrier_who_served_the_biggest_number_of_flights.sql
}

