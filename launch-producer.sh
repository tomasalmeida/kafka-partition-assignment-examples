#!/usr/bin/env bash
TOPIC_PREFIX=topic

if [[ $# -eq 0 ]] ; then
    echo 'Provide the number of topics and partitions'
    echo "$0 <topics> <number-of-partitions>"
    exit 0
fi

TOPICS=$1
PARTITIONS=$2

docker-compose down -v
docker-compose up -d

ALL_TOPICS=()
for ((topic=0; topic< $TOPICS; topic++)); do
  TOPIC_NAME="$TOPIC_PREFIX-$topic"
  ALL_TOPICS+=($TOPIC_NAME)
  kafka-topics --bootstrap-server localhost:29092 --create --topic $TOPIC_NAME --partitions $PARTITIONS --replication-factor 1
  kafka-producer-perf-test --topic $TOPIC_NAME --num-records 600000 --record-size 100 --throughput 10000 --producer-props bootstrap.servers=localhost:29092
done
ALL_TOPICS=`echo ${ALL_TOPICS[@]} | tr ' ' ','`


cd tooling
mvn clean package
java -cp target/partitioning-tool-1.0.0-SNAPSHOT-jar-with-dependencies.jar partitioning.tool.kafka.producer.ProducerStarter config.properties 10 "$ALL_TOPICS"


echo "***********************************"
echo "* REMEMBER TO DESTROY THE CLUSTER *"
echo " >>>  docker-compose down -v  <<< *"
echo "***********************************"

