#!/usr/bin/env bash

if [[ $# -ne 1 ]] ; then
    echo 'Provide the consumer group to check status'
    echo "$0 <consumer-group> "
    exit 0
fi

CONSUMER_GROUP=$1

watch -n1 "kafka-consumer-groups --bootstrap-server localhost:29092 --group $CONSUMER_GROUP --describe --members --verbose && \
           kafka-consumer-groups --bootstrap-server localhost:29092 --group $CONSUMER_GROUP --describe --offsets --verbose"