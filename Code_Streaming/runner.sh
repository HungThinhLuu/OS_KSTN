#!/bin/bash

gnome-terminal --title=Run_Zookeeper -- sudo /opt/kafka/bin/zookeeper-server-start.sh /opt/kafka/config/zookeeper.properties
sleep 10s
gnome-terminal --title=Run_Server -- sudo /opt/kafka/bin/kafka-server-start.sh /opt/kafka/config/server.properties
sleep 10s
gnome-terminal --title=Make_Topic -- sudo bin/kafka-topics.sh --create --topic myTest --bootstrap-server localhost:9092
