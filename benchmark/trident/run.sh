#!/bin/sh

# global configurations

CUR_DIR=`dirname "$0"`
BASE_DIR=$CUR_DIR/..
PRODUCER_DIR=$BASE_DIR/producer/fileread
. $BASE_DIR/conf/config.sh
. $BASE_DIR/bin/functions.sh

# topology configuraitons

TOPOLOGY_CLASS=TridentWordCount
TOPOLOGY_NAME=TridentWordCount

PRODUCER_NAME=KafkaFileReadProducer
TOPIC="fileread"
CLIENT_ID="trident"

WORKERS=4
ACKERS=$WORKERS
PENDING=200

COMPONENT=topology.component
SPOUT_NUM=4
SPLIT_NUM=8
COUNT_NUM=12

TOPOLOGY_CONF=$TOPOLOGY_CONF,topology.name=$TOPOLOGY_NAME,topology.workers=$WORKERS,topology.acker.executors=$ACKERS,topology.max.spout.pending=$PENDING,$COMPONENT.spout_num=$SPOUT_NUM,$COMPONENT.split_bolt_num=$SPLIT_NUM,$COMPONENT.count_bolt_num=$COUNT_NUM
KAFKA_CONF=$KAFKA_CONF,topic=$TOPIC,client_id=$CLIENT_ID

echo "========== running TridentWordCount =========="
run_producer
run_benchmark
kill_producer
