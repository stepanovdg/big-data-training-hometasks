#!/usr/bin/env bash
set -ex

SOURCE="${BASH_SOURCE[0]}"
while [ -h "$SOURCE" ]; do # resolve $SOURCE until the file is no longer a symlink
  DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
  SOURCE="$(readlink "$SOURCE")"
  [[ $SOURCE != /* ]] && SOURCE="$DIR/$SOURCE" # if $SOURCE was a relative symlink, we need to resolve it relative to the path where the symlink file was located
done
DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"

ENV=${DIR}/run
FORCE="0";

source ${DIR}/global-functions.sh

displayHelp(){
	echo "Parameters:
	-t|--task     -> To run task number,
	-f|--force   -> To run without setup,

	Examples:
		$0 --task 1 -f true"
}

function checkInout(){


if [ -z ${TASK+x} ]; then
		echo "Need to specify task number";
		displayHelp
		exit 1;
fi

sourceByTask ${TASK}

}

function sourceByTask(){
  local task=$1
  source ${DIR}/task${task}/setup.sh
  source ${DIR}/task${task}/run.sh
}

function setupIfRequired(){
  local ret;

  if $(checkRequired );
    then
      echo "Do not required to setup"
    else
      if $(loadDataForTables);
        then
          $(setupTables);
        else
          echo "failed to load data"
      fi
  fi
}

function run(){
  runChild;
}

while [[ $# -gt 1 ]]
do
key="$1"

case $key in
    -t|--task)
    TASK="$2"
    shift # past argument
    ;;
    -f|--force)
    FORCE="1"
    shift # past argument
    ;;
    *)
		displayHelp
	;;
esac
shift # past argument or value
done

checkInout;
if [[ "${FORCE}" == "1" ]];
 then
		echo "Setup is not required";
  else
		echo "Checking required setup";
		setupIfRequired;
fi
run;