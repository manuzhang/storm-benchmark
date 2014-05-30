#!/bin/sh

APP_ID=application_1400224450350_0061
JAR=/root/storm-benchmark-0.9.0-Intel-jar-with-dependencies.jar
MAIN_CLASS=storm.benchmark.BenchmarkRunner

METRICS_POLL_INTERVAL=60000 # 60 secs
METRICS_TOTAL_TIME=300000  # 5 mins
METRICS_PATH=/root/benchmark/reports
METRICS_CONF=metrics.time=$METRICS_TOTAL_TIME,metrics.poll=$METRICS_POLL_INTERVAL,metrics.path=$METRICS_PATH

BROKER_LIST=intelidh-04:9092
ZOOKEEPER_SERVERS=intelidh-04:2181
KAFKA_ROOT_PATH=/kafka/kafka-cluster-0

KAFKA_CONF=broker.list=$BROKER_LIST,zookeeper.servers=$ZOOKEEPER_SERVERS,kafka.root.path=$KAFKA_ROOT_PATH
