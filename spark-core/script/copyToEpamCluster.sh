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

sshdp='ssh -p 22 -oStrictHostKeyChecking=no devuser@cluster'

CLUST_DIR=/home/devuser/training/spark-core
HDFS_DIR=/user/stepanov/spark-core/

function  schdp() {
  ${sshdp} "mkdir -p ${CLUST_DIR}"
	scp -P 22 -oStrictHostKeyChecking=no -r $1 devuser@cluster:${CLUST_DIR} &
}

schdp ${DIR}/../target/spark-core-1.0-SNAPSHOT.jar
#schdp /home/dstepanov/projects/BigData/bigdata-training/dev/homeworks/spark-core/dataset/local

wait

if [ -z ${1+x} ]; then
		exit 0;
fi


#${sshdp} hdfs dfs -mkdir -p ${HDFS_DIR}
#${sshdp} hdfs dfs -put -f ${CLUST_DIR}/local/bids.txt ${HDFS_DIR}
#${sshdp} hdfs dfs -put -f ${CLUST_DIR}/local/exchange_rate.txt ${HDFS_DIR}
#${sshdp} hdfs dfs -put -f ${CLUST_DIR}/local/motels.txt ${HDFS_DIR}
${sshdp}  hdfs dfs -rm -r -f -skipTrash ${HDFS_DIR}/spark-core-output
${sshdp} << EOF
  export SPARK_MAJOR_VERSION=2
  /usr/hdp/current/spark2-client/bin/spark-submit --master yarn --deploy-mode cluster --driver-memory 510mb --num-executors 6 --executor-memory 510mb --conf spark.executor.cores=1 \
  ${CLUST_DIR}/spark-core-1.0-SNAPSHOT.jar \
  hdfs:///${HDFS_DIR}/bids.txt \
  hdfs:///${HDFS_DIR}/motels.txt \
  hdfs:///${HDFS_DIR}/exchange_rate.txt \
  hdfs:///${HDFS_DIR}/spark-core-output
EOF
