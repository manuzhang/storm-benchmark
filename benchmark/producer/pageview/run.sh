#!/bin/sh

# global configurations

CUR_DIR=`dirname "$0"`
BASE_DIR=$CUR_DIR/../..
. $BASE_DIR/conf/config.sh
. $BASE_DIR/bin/functions.sh

# topology configuraitons

TOPOLOGY_CLASS=KafkaPageViewProducer
TOPOLOGY_NAME=KafkaPageViewProducer

TOPIC="pageview"
WORKERS=4
ACKERS=$WORKERS
PENDING=200

COMPONENT=producer
SPOUT_NUM=4
BOLT_NUM=4

TOPOLOGY_CONF=$TOPOLOGY_CONF,topology.name=$TOPOLOGY_NAME,topology.workers=$WORKERS,topology.acker.executors=$ACKERS,topology.max.spout.pending=$PENDING,$COMPONENT.spout_num=$SPOUT_NUM,$COMPONENT.bolt_num=$BOLT_NUM
KAFKA_CONF=$KAFKA_CONF,topic=$TOPIC

echo "========== running KafkaPageViewProducer =========="
check
run_topology

