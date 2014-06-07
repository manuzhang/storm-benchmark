#!/bin/sh

# global configurations

CUR_DIR=`dirname "$0"`
BASE_DIR=$CUR_DIR/..
PRODUCER_DIR=$BASE_DIR/producer/pageview
. $BASE_DIR/conf/config.sh
. $BASE_DIR/bin/functions.sh

# topology configurations

TOPOLOGY_CLASS=PageViewCount
TOPOLOGY_NAME=PageViewCount

PRODUCER_NAME=KafkaPageViewProducer
TOPIC="pageview"
CLIENT_ID="pageview_count"

WORKERS=4
ACKERS=$WORKERS
PENDING=200

COMPONENT=topology.component
SPOUT_NUM=4
VIEW_NUM=8
COUNT_NUM=8


TOPOLOGY_CONF=$TOPOLOGY_CONF,topology.name=$TOPOLOGY_NAME,topology.workers=$WORKERS,topology.acker.executors=$ACKERS,topology.max.spout.pending=$PENDING,$COMPONENT.spout_num=$SPOUT_NUM,$COMPONENT.view_bolt_num=$VIEW_NUM,$COMPONENT.count_bolt_num=$COUNT_NUM
KAFKA_CONF=$KAFKA_CONF,topic=$TOPIC,client_id=$CLIENT_ID

echo "========== running PageViewCount =========="
run_producer
run_benchmark
kill_producer
