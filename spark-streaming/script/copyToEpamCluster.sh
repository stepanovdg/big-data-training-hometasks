#!/usr/bin/env bash
set -ex

SOURCE="${BASH_SOURCE[0]}"
while [ -h "$SOURCE" ]; do # resolve $SOURCE until the file is no longer a symlink
  DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
  SOURCE="$(readlink "$SOURCE")"
  [[ $SOURCE != /* ]] && SOURCE="$DIR/$SOURCE" # if $SOURCE was a relative symlink, we need to resolve it relative to the path where the symlink file was located
done
DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"


ENV=${DIR}/../src/main/resources/conf/config
source ${DIR}/common.sh

cluster1=$2

sshdp='ssh -p 22 -oStrictHostKeyChecking=no devuser@cluster'

BROKER_HOST_PORT=$(prop 'bootstrap.servers')
ZOOKEEPER_URL=$(prop 'zookeeper.url')
TOPIC_IN=$(prop 'raw.topic')
TOPIC_OUT=$(prop 'enriched.topic')
KAFKA_HOME=/usr/hdp/current/kafka-broker
CLUST_DIR=/home/devuser/training/spark-streaming
HDFS_DIR=/user/stepanov/spark-streaming/

function  schdp() {
  ${sshdp} "mkdir -p ${CLUST_DIR}"
	scp -P 22 -oStrictHostKeyChecking=no -r $1 devuser@cluster:${CLUST_DIR} &
}

function  schdpZeppelin() {
  ${sshdp} "mkdir -p ${CLUST_DIR}"
	scp -P 22 -oStrictHostKeyChecking=no -r $1 devuser@ecsc00a016c7.epam.com:/usr/hdp/current/zeppelin-server/local-repo/2DB8EMZR4/ &
}
wait

if [ -z ${1+x} ]; then
		exit 0;
fi

schdp ${DIR}/../target/spark-streaming-1.0-SNAPSHOT.jar
schdpZeppelin ${DIR}/../target/spark-streaming-1.0-SNAPSHOT.jar
schdp ${DIR}/../target/spark-streaming-1.0-SNAPSHOT-jar-with-dependencies.jar
schdp ${DIR}/../data_small/one_device_2015-2017.csv
#schdp ${DIR}/../data/one_device_2015-2017.csv

# ${CLUST_DIR}/spark-streaming-1.0-SNAPSHOT.jar > /tmp/out.txt &
#${KAFKA_HOME}/bin/kafka-console-producer.sh --broker-list ${BROKER_HOST_PORT} --topic test


#${sshdp} ${KAFKA_HOME}/bin/kafka-topics.sh --zookeeper ${ZOOKEEPER_URL} --delete --if-exists --topic ${TOPIC_IN}
#${sshdp} ${KAFKA_HOME}/bin/kafka-topics.sh --zookeeper ${ZOOKEEPER_URL} --delete --if-exists --topic ${TOPIC_OUT}
${sshdp} ${KAFKA_HOME}/bin/kafka-topics.sh --zookeeper ${ZOOKEEPER_URL} --delete --if-exists --topic monitoringEnriched2_stepanov

#${sshdp} ${KAFKA_HOME}/bin/kafka-topics.sh --zookeeper ${ZOOKEEPER_URL} --create --if-not-exists --topic ${TOPIC_IN} --replication-factor 1 --partitions 10
#${sshdp} ${KAFKA_HOME}/bin/kafka-topics.sh --zookeeper ${ZOOKEEPER_URL} --create --if-not-exists --topic ${TOPIC_OUT} --replication-factor 1 --partitions 1
${sshdp} ${KAFKA_HOME}/bin/kafka-topics.sh --zookeeper ${ZOOKEEPER_URL} --list

#/usr/hdp/current/kafka-broker/bin/kafka-console-consumer.sh --zookeeper ecsc00a01699.epam.com:2181,ecsc00a016c7.epam.com:2181,ecsc00a01782.epam.com:2181,ecsc00a0190c.epam.com:2181 --topic monitoring20stepanov --from-beginning
#/usr/hdp/current/kafka-broker/bin/kafka-console-consumer.sh --zookeeper ecsc00a01699.epam.com:2181,ecsc00a016c7.epam.com:2181,ecsc00a01782.epam.com:2181,ecsc00a0190c.epam.com:2181 --topic monitoringEnriched2stepanov --from-beginning
#${sshdp} nohup ${KAFKA_HOME}/bin/kafka-console-consumer.sh --zookeeper ${ZOOKEEPER_URL} --topic ${TOPIC_OUT} --from-beginning &



#${sshdp} java -jar ${CLUST_DIR}/spark-streaming-1.0-SNAPSHOT-jar-with-dependencies.jar ./training/spark-streaming/one_device_2015-2017.csv



