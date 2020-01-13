#!/usr/bin/env bash

/root/bigdata/kafka/bin/zookeeper-server-start.sh -daemon /root/bigdata/kafka/config/zookeeper.properties

/root/bigdata/kafka/bin/kafka-server-start.sh /root/bigdata/kafka/config/server.properties

/root/bigdata/kafka/bin/kafka-topics.sh --zookeeper 192.168.19.137:2181 --create --replication-factor 1 --topic click-trace --partitions 1