# Partition assignment example

See the behavior of the partition assignment in different configuration

## Pre-requisites
* docker-compose
* [Confluent CLI](https://docs.confluent.io/platform/current/installation/installing_cp/zip-tar.html)
* watch command

## How to run

In one terminal, start the cluster and produce:

     ./launch-producer.sh <number of topics> <partitions per topic>

In another terminal, check the status

    ./status.sh <consumer-group>

And in another terminal, launch the consumer group

     ./launch-consumer.sh <strategy> <initial consumers> <consumer-group>


### Clean up

Stop all scripts
Run

    docker-compose down -v
