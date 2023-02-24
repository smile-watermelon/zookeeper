#!/bin/bash

case $1 in
"start"){
    for host in hadoop-01 hadoop-02 hadoop-03; do
        echo ----------- $host start ------------
        ssh $host "/opt/module/zookeeper-3.6.3/bin/zkServer.sh start"
    done
};;
"stop") {
    for host in hadoop-01 hadoop-02 hadoop-03; do
        echo ----------- $host stop ------------
        ssh $host "/opt/module/zookeeper-3.6.3/bin/zkServer.sh stop"
    done
};;
"status") {
    for host in hadoop-01 hadoop-02 hadoop-03; do
        echo ----------- $host status ------------
        ssh $host "/opt/module/zookeeper-3.6.3/bin/zkServer.sh status"
    done
};;
esac