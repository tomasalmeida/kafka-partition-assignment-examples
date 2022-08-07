# Partition assignment example

See the behavior of the partition assignment in different configuration

## Pre-requisites
* docker-compose
* [Confluent CLI](https://docs.confluent.io/platform/current/installation/installing_cp/zip-tar.html)

## How to run

In one terminal, start the cluster and produce:

     ./launch-producer.sh <number of topics> <partitions per topic>

In another terminal, check the status

    ./status.sh <consumer-group>

And in another terminal, launch the consumer group

     ./launch-consumer.sh <strategy> <initial consumers> <consumer-group> <static-assignment>

Strategies can be:
* round: https://kafka.apache.org/32/javadoc/org/apache/kafka/clients/consumer/RoundRobinAssignor.html
* range: https://kafka.apache.org/32/javadoc/org/apache/kafka/clients/consumer/RangeAssignor.html
* sticky: https://kafka.apache.org/32/javadoc/org/apache/kafka/clients/consumer/StickyAssignor.html
* coop: https://kafka.apache.org/32/javadoc/org/apache/kafka/clients/consumer/CooperativeStickyAssignor.html

`static-assignment` can be true or false (false by default) (timeout is 10 seconds)

### Clean up

1. Stop all scripts
2. Run

    docker-compose down -v
