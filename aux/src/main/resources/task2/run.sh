#!/usr/bin/env bash

function runChild(){
  cat ${DIR}/task2/linux_messages_3000lines.txt | while read line ; do echo "$line" ; sleep 0.2 ; done > $(prop 'input_file')
}