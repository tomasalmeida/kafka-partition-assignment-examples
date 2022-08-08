# Partition assignment example

See the behavior of the partition assignment in different configuration

## Pre-requisites
* docker-compose
* [Confluent CLI](https://docs.confluent.io/platform/current/installation/installing_cp/zip-tar.html)

## How to run

### 1. Producer

In one terminal, start the cluster and produce:

     ./launch-producer.sh <number of topics> <partitions per topic>


Example:

      ./launch-producer.sh 4 3

### 2. Status
In another terminal, check the status

    ./status.sh <consumer-group>

Example:

      ./status.sh beta1

      Date: 2022-08-07T12:19:24.260758 - consumerGroup: beta1

      Topic             Partition   currentOffset   endOffset        Client Id	instanceId
      topic-0           0           82552           115740           DOG-sssss	consumer-0
      topic-0           1           82734           123495           DOG-sssss	consumer-0
      topic-0           2           82829           124778           DOG-sssss	consumer-0
      topic-0           3           83019           132598           DOG-sssss	consumer-0
      topic-0           4           82159           104783           DOG-sssss	consumer-0
      topic-1           0           90748           102191           DOG-sssss	consumer-0
      topic-1           1           82012           111033           DOG-sssss	consumer-0
      topic-1           2           91547           121748           DOG-sssss	consumer-0
      topic-1           3           64296           130536           DOG-sssss	consumer-0
      topic-1           4           60149           135886           DOG-lllll	consumer-1
      topic-2           0           45272           101025           DOG-sssss	consumer-0
      topic-2           1           44761           111701           DOG-lllll	consumer-1
      topic-2           2           54954           121007           DOG-sssss	consumer-0
      topic-2           3           73511           130747           DOG-lllll	consumer-1
      topic-2           4           73653           136913           DOG-lllll	consumer-1
      topic-3           0           72045           95246            DOG-lllll	consumer-1
      topic-3           1           72594           107364           DOG-lllll	consumer-1
      topic-3           2           73044           121535           DOG-lllll	consumer-1
      topic-3           3           73271           132366           DOG-lllll	consumer-1
      topic-3           4           73589           144882           DOG-lllll	consumer-1
      topic-4           0           72036           95205            DOG-lllll	consumer-1
      topic-4           1           72662           110642           DOG-lllll	consumer-1
      topic-4           2           36530           119117           DOG-lllll	consumer-1
      topic-4           3           36726           132765           DOG-sssss	consumer-0
      topic-4           4           36807           143664           DOG-lllll	consumer-1
      
      CLIENT           PARTITIONS
      DOG-lllll        topic-1(4) topic-2(1,3,4) topic-3(0,1,2,3,4) topic-4(0,1,2,4)
      DOG-sssss        topic-0(0,1,2,3,4) topic-1(0,1,2,3) topic-2(0,2) topic-4(3)

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

      ./launch-consumer.sh coop 0 beta1 true
      
      What's next? [a] Add new consumer, [k] kill last consumer, [e] exit: a
      consumer-0 launched [71714]
      
      What's next? [a] Add new consumer, [k] kill last consumer, [e] exit: a
      consumer-1 launched [71717]
      
      What's next? [a] Add new consumer, [k] kill last consumer, [e] exit: a
      consumer-2 launched [71726]
      
      What's next? [a] Add new consumer, [k] kill last consumer, [e] exit: k
      Consumer 2 killed (pid 71726) - Date: Sun Aug  7 12:18:52 CEST 2022
      
      What's next? [a] Add new consumer, [k] kill last consumer, [e] exit: e
      Time to clean up!
      Consumer 0 killed (pid 71714) - Date: Sun Aug  7 12:22:47 CEST 2022
      Consumer 1 killed (pid 71717) - Date: Sun Aug  7 12:22:47 CEST 2022

### Clean up

1. Stop all scripts
2. Run

    docker-compose down -v
