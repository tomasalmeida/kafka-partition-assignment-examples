# Partition assignment example

See the behavior of the partition assignment in different configuration

## Pre-requisites
* docker-compose
* [Confluent CLI](https://docs.confluent.io/platform/current/installation/installing_cp/zip-tar.html)

## How to run

### 1. Producer

In one terminal, start the cluster and producer (you will be asked some questions):

     ./launch-producer.sh

> For each tipic, we will feed it with 600000 records, so it can take some time :-) 

Example:

      ./launch-producer.sh
      [+] Running 3/3
      ⠿ Network kafka-partition-assignment-examples_default  Created                <-- cluster being started                                                                                                                      0.0s
      ⠿ Container zookeeper                                  Started                                                                                                                                      0.4s
      ⠿ Container broker                                     Started                                                                                                                                      0.7s
      
      Should I create a new topic? [y/n]: y
      How many partitions? 1
      Created topic topic-0.
      
      Should I create a new topic? [y/n]: y
      How many partitions? 3
      Created topic topic-1.
      
      Should I create a new topic? [y/n]: y
      How many partitions? NaN
      error: 'NaN' is not a valid number
      Start again :-(
      
      Should I create a new topic? [y/n]: y
      How many partitions? 2
      Created topic topic-2.
      
      Should I create a new topic? [y/n]: n
      all topics = topic-0,topic-1,topic-2
      
      ... # producer will be started

### 2. Status
In another terminal, check the status

    ./status.sh <consumer-group>

Example:

      ./status.sh g2
      Date: 2022-08-25T23:39:24.017862 - consumerGroup: g2

      Topic     Partition   currentOffset   end Offset   Client Id      instanceId
      topic-0        0             27110       114036    consumer-0     consumer-0
      topic-0        1             27703       121839    consumer-1     consumer-1
      topic-0        2             27766       127915    consumer-2     consumer-2
      topic-0        3             27759       132468    consumer-3     consumer-3
      topic-0        4             18281       103742    consumer-4     consumer-4
      topic-1        0             20061        45374    consumer-0     consumer-0
      topic-1        1             18033        50980    consumer-0     consumer-0
      topic-1        2             18071        52326    consumer-1     consumer-1
      topic-1        3             19474        56567    consumer-1     consumer-1
      topic-1        4             19761        58721    consumer-2     consumer-2
      topic-1        5             18249        60598    consumer-2     consumer-2
      topic-1        6             18294        63834    consumer-3     consumer-3
      topic-1        7             19219        69336    consumer-3     consumer-3
      topic-1        8             22302        69547    consumer-4     consumer-4
      topic-1        9             18417        72717    consumer-4     consumer-4
      
      CLIENT		PARTITIONS
      consumer-0	topic-0(0) topic-1(0,1)
      consumer-1	topic-0(1) topic-1(2,3)
      consumer-2	topic-0(2) topic-1(4,5)
      consumer-3	topic-0(3) topic-1(6,7)
      consumer-4	topic-0(4) topic-1(8,9)

### 3. Consumers

And in another terminal, launch the consumer group

     ./launch-consumer.sh <strategy> <initial consumers> <consumer-group> <static-assignment>

Strategies can be:
* round: https://kafka.apache.org/32/javadoc/org/apache/kafka/clients/consumer/RoundRobinAssignor.html
* range: https://kafka.apache.org/32/javadoc/org/apache/kafka/clients/consumer/RangeAssignor.html
* sticky: https://kafka.apache.org/32/javadoc/org/apache/kafka/clients/consumer/StickyAssignor.html
* coop: https://kafka.apache.org/32/javadoc/org/apache/kafka/clients/consumer/CooperativeStickyAssignor.html

`static-assignment` can be true or false (false by default) (timeout is 10 seconds)

Example:

      $ ./launch-consumer.sh range 3 r3 true
      consumer-0 launched [11137]
      consumer-1 launched [11138]
      consumer-2 launched [11139]
      
      What's next? [a] Add new consumer, [k] kill a consumer, [e] exit: a
      Give the consumer number you want to create: 3
      consumer-3 launched [11160]
      
      What's next? [a] Add new consumer, [k] kill a consumer, [e] exit: a
      Give the consumer number you want to create: 4
      consumer-4 launched [11170]
      
      What's next? [a] Add new consumer, [k] kill a consumer, [e] exit: k
      Give the consumer number you want to kill: 3
      Consumer 3 killed (pid 11160) - Date: Thu Aug 25 23:31:01 CEST 2022
      
      What's next? [a] Add new consumer, [k] kill a consumer, [e] exit: k
      Give the consumer number you want to kill: 2
      Consumer 2 killed (pid 11139) - Date: Thu Aug 25 23:31:16 CEST 2022
      
      What's next? [a] Add new consumer, [k] kill a consumer, [e] exit: e
      Time to clean up!
      consumers 4
      Consumer 0 killed (pid 11137) - Date: Thu Aug 25 23:31:17 CEST 2022
      Consumer 1 killed (pid 11138) - Date: Thu Aug 25 23:31:17 CEST 2022
      No pid found for consumer 2.
      No pid found for consumer 3.
      Consumer 4 killed (pid 11170) - Date: Thu Aug 25 23:31:17 CEST 2022

### Clean up

1. Stop all scripts
2. Run

    docker-compose down -v

# References:
- https://medium.com/streamthoughts/understanding-kafka-partition-assignment-strategies-and-how-to-write-your-own-custom-assignor-ebeda1fc06f3
