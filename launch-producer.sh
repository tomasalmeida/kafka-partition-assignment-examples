#!/usr/bin/env bash
TOPIC_PREFIX=topic

cd tooling
mvn clean package
cd ..

# start cluster
docker-compose down -v
docker-compose up -d

#
cd tooling
mvn clean package
cd ..

ALL_TOPICS=()
TOPIC=0
while [ "$CREATE_TOPIC" != "n" ]; do
  echo ""
  echo -n "Should I create a new topic? [y/n]: "
  read -n1 CREATE_TOPIC
  echo ""
  if [ "$CREATE_TOPIC" == "y" ]; then
    echo -n "How many partitions? "
    read PARTITIONS
    if ! [[ $PARTITIONS -gt 0 ]]; then
      echo "error: '$PARTITIONS' is not a valid number" >&2
      echo "Start again :-(" >&2
    else
      TOPIC_NAME="$TOPIC_PREFIX-$TOPIC"
      ALL_TOPICS+=($TOPIC_NAME)
      kafka-topics --bootstrap-server localhost:29092 --create --topic $TOPIC_NAME --partitions $PARTITIONS --replication-factor 1
      echo "Adding initial data..."
      kafka-producer-perf-test --topic $TOPIC_NAME --num-records 600000 --record-size 100 --throughput 10000 --producer-props bootstrap.servers=localhost:29092
      let TOPIC++
    fi
  fi
done
ALL_TOPICS=$(echo ${ALL_TOPICS[@]} | tr ' ' ',')
echo "all topics = $ALL_TOPICS"

cd tooling
java -cp target/partitioning-tool-1.0.0-SNAPSHOT-jar-with-dependencies.jar partitioning.tool.kafka.producer.ProducerStarter config.properties 10 "$ALL_TOPICS"

echo "***********************************"
echo "* REMEMBER TO DESTROY THE CLUSTER *"
echo " >>>  docker-compose down -v  <<< *"
echo "***********************************"
