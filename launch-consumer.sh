#!/usr/bin/env bash

REGEX_NUMBER='^[0-9]+$'

show_help() {
  echo "Provide"
  echo "   Strategy can be: round, range, sticky, coop"
  echo "   number of first consumers"
  echo "   consumer group"
  echo "   instance id assignment: true/false (false by default)"
  echo ""
  echo "$0 <strategy> <number-of-consumers> <consumer-group> <instanceId-assignment>"
}

launch_consumer() {
  ID=$1
  INSTANCE="consumer-${ID}"
  java -cp target/partitioning-tool-1.0.0-SNAPSHOT-jar-with-dependencies.jar \
    partitioning.tool.kafka.consumer.ConsumerStarter \
    config.properties $GROUP_ID \
    $STRATEGY_CLASS $INSTANCE $SET_STATIC &>/dev/null &
  consumers[$ID]=$!
  echo "consumer-$ID launched [${consumers[$ID]}]"
}

kill_consumer() {
  ID=$1
  PID="${consumers[ID]}"
  if [[ PID -gt 0 ]]; then
    kill $PID
    echo "Consumer $ID killed (pid $PID) - Date: $(date)"
    consumers[$ID]=0
  else
    echo "No pid found for consumer $ID."
  fi
}

if [[ $# -lt 3 ]]; then
  show_help
  exit 0
fi

case "$1" in
"round")
  STRATEGY_CLASS="org.apache.kafka.clients.consumer.RoundRobinAssignor"
  ;;
"range")
  STRATEGY_CLASS="org.apache.kafka.clients.consumer.RangeAssignor"
  ;;
"sticky")
  STRATEGY_CLASS="org.apache.kafka.clients.consumer.StickyAssignor"
  ;;
"coop")
  STRATEGY_CLASS="org.apache.kafka.clients.consumer.CooperativeStickyAssignor"
  ;;
*)
  echo "Strategy not recognised"
  exit 1
  ;;
esac

TOTAL_CONSUMERS=$2
GROUP_ID=$3
SET_STATIC=$(echo $4 | tr 'A-Z' 'a-z')
SET_STATIC=${SET_STATIC:='false'}
cd tooling

declare -a consumers
for ((i = 0; i < TOTAL_CONSUMERS; i++)); do
  launch_consumer $i
done

while [ "$action_letter" != "e" ]; do
  echo ""
  echo -n "What's next? [a] Add new consumer, [k] kill last consumer, [e] exit: "
  read -n1 action_letter
  echo ""

  case "$action_letter" in
  "a")
    launch_consumer $TOTAL_CONSUMERS
    let TOTAL_CONSUMERS++
    ;;
  "k")
    echo -n "Give the consumer number you want to kill:  "
    read ID
    if [[ $ID =~ $REGEX_NUMBER ]]; then
      kill_consumer $ID
    else
      echo "Not a number to me :-("
    fi
    ;;
  "e")
    echo "Time to clean up!"
    ;;
  *)
    echo "Command not recognised!"
    ;;
  esac

done

for ((i = 0; i < TOTAL_CONSUMERS; i++)); do
  kill_consumer $i
done
