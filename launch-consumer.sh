#!/usr/bin/env bash
if [[ $# -ne 3 ]] ; then
    echo "Provide"
    echo "   Strategy can be: round, range, sticky, coop"
    echo "   number of first consumers"
    echo "   consumer group"
    echo "$0 <strategy> <number-of-consumers> <consumer-group>"
    exit 0
fi

INITIAL_CONSUMERS=$2
GROUP_ID=$3

case "$1" in
   "round") STRATEGY_CLASS="org.apache.kafka.clients.consumer.RoundRobinAssignor"
   ;;
   "range") STRATEGY_CLASS="org.apache.kafka.clients.consumer.RangeAssignor"
   ;;
   "sticky") STRATEGY_CLASS="org.apache.kafka.clients.consumer.StickyAssignor"
   ;;
   "coop") STRATEGY_CLASS="org.apache.kafka.clients.consumer.CooperativeStickyAssignor"
   ;;
   *)
     echo "Strategy not recognised"
     exit 1
   ;;
esac

cd tooling

declare -a consumers
for i in $(seq $INITIAL_CONSUMERS); do
  java -cp target/partitioning-tool-1.0.0-SNAPSHOT-jar-with-dependencies.jar \
        partitioning.tool.kafka.consumer.ConsumerStarter \
        config.properties $GROUP_ID "topic-.*" 30 \
        $STRATEGY_CLASS &>/dev/null &
#  kafka-console-consumer \
#      --bootstrap-server localhost:29092 \
#      --group $GROUP_ID \
#      --include "topic-.*" \
#      --consumer-property consumer.id="consumer_${i}" \
#      --consumer-property partition.assignment.strategy=$STRATEGY_CLASS \
#      --from-beginning &>/dev/null &

  consumers[$i]=$!
  echo "Consumer $i launched [${consumers[$i]}]"
done

echo -n "Add one more consumer. [Press any key]"
read

java -cp target/partitioning-tool-1.0.0-SNAPSHOT-jar-with-dependencies.jar \
      partitioning.tool.kafka.consumer.ConsumerStarter \
      config.properties $GROUP_ID "topic-.*" 30 \
      $STRATEGY_CLASS &>/dev/null &
#kafka-console-consumer \
#    --bootstrap-server localhost:29092 \
#    --group $GROUP_ID \
#    --include "topic-.*" \
#    --consumer-property consumer.id="consumer_${TOTAL}" \
#    --consumer-property partition.assignment.strategy=$STRATEGY_CLASS \
#    --from-beginning &>/dev/null &
LAST_CONSUMER=$!
echo "Extra launched [${LAST_CONSUMER}]"

echo -n "> Confirm kill extra consumer. [Press any key]"
read
kill $LAST_CONSUMER
echo ">> killed $LAST_CONSUMER"

echo -n "> Confirm kill all consumers. [Press any key]"
read

for i in $(seq $INITIAL_CONSUMERS); do
  pid="${consumers[i]}"
  kill $pid
  echo ">> killed $pid"
done