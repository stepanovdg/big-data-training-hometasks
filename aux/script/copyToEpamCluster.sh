#!/usr/bin/env bash
set -ex

SOURCE="${BASH_SOURCE[0]}"
while [ -h "$SOURCE" ]; do # resolve $SOURCE until the file is no longer a symlink
  DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
  SOURCE="$(readlink "$SOURCE")"
  [[ $SOURCE != /* ]] && SOURCE="$DIR/$SOURCE" # if $SOURCE was a relative symlink, we need to resolve it relative to the path where the symlink file was located
done
DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"

cluster1=$2

sshdp='ssh -p 22 -oStrictHostKeyChecking=no root@cluster'
#sshdp='ssh -p 2222 -oStrictHostKeyChecking=no root@sandbox.hortonworks.com'

function  schdp() {
  ${sshdp} 'mkdir -p /home/devuser/training/'
	scp -P 22 -oStrictHostKeyChecking=no -r $1 root@cluster:/home/devuser/training/ &
#	scp -P 2222 -oStrictHostKeyChecking=no -r $1 root@sandbox.hortonworks.com:/root/training/ &
}

schdp ${DIR}/../src/main/resources/
schdp ${DIR}/../heavy/

wait

if [ -z ${1+x} ]; then
		exit 0;
fi

${sshdp} 'chmod +x /home/devuser/training/resources/entry-point.sh'
${sshdp} /home/devuser/training/resources/entry-point.sh -t 2 -s 0 -f true
#${sshdp} /home/devuser/training/resources/entry-point.sh -t 3 -s 0