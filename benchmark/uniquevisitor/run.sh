#!/bin/sh

# global configurations 

CUR_DIR=`dirname "$0"`
BASE_DIR=$CUR_DIR/..
PRODUCER_DIR=$BASE_DIR/producer/pageview
. $BASE_DIR/conf/config.sh
. $BASE_DIR/bin/functions.sh

# topology configurations

TOPOLOGY_CLASS=UniqueVisitor
TOPOLOGY_NAME=UniqueVisitor

PRODUCER_NAME=KafkaPageViewProducer
TOPIC="pageview"
CLIENT_ID="unique_visitor"

WORKERS=4
ACKERS=$WORKERS
PENDING=200

COMPONENT=topology.component
SPOUT_NUM=4
VIEW_NUM=8
UNIQUER_NUM=8

WINDOW_LENGTH=300 # 5 mins
EMIT_FREQ=60      # 60 secs


TOPOLOGY_CONF=$TOPOLOGY_CONF,topology.name=$TOPOLOGY_NAME,topology.workers=$WORKERS,topology.acker.executors=$ACKERS,topology.max.spout.pending=$PENDING,$COMPONENT.spout_num=$SPOUT_NUM,$COMPONENT.view_bolt_num=$VIEW_NUM,$COMPONENT.uniquer_bolt_num=$COUNT_NUM,window.length=$WINDOW_LENGTH,emit.frequency=$EMIT_FREQ
KAFKA_CONF=$KAFKA_CONF,topic=$TOPIC,client_id=$CLIENT_ID

echo "========== running UniqueVisitor =========="
run_producer
run_benchmark
kill_producer
