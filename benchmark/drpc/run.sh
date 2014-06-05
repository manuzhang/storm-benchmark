#!/bin/sh

# global configurations

CUR_DIR=`dirname "$0"`
BASE_DIR=$CUR_DIR/..
PRODUCER_DIR=$BASE_DIR/producer/pageview
. $BASE_DIR/conf/config.sh
. $BASE_DIR/bin/functions.sh

# topology configurations

TOPOLOGY_CLASS=DRPC
TOPOLOGY_NAME=DRPC

PRODUCER_NAME=KafkaPageViewProducer
TOPIC="pageview"
CLIENT_ID="drpc"

WORKERS=4
ACKERS=$WORKERS
PENDING=200

COMPONENT=topology.component
SPOUT_NUM=3
PAGE_NUM=6
VIEW_NUM=9
USER_NUM=4
FOLLOWER_NUM=8

DRPC_SERVER=intelidh-01
DRPC_PORT=54598

TOPOLOGY_CONF=$TOPOLOGY_CONF,topology.name=$TOPOLOGY_NAME,topology.workers=$WORKERS,topology.acker.executors=$ACKERS,topology.max.spout.pending=$PENDING,drpc.server=$DRPC_SERVER,drpc.port=$DRPC_PORT,$COMPONENT.spout_num=$SPOUT_NUM,$COMPONENT.page_bolt_num=$PAGE_NUM,$COMPONENT.view_bolt_num=$VIEW_NUM,$component.user_bolt_num=$USER_NUM,$component.follower_bolt_num=$FOLLOWER_NUM
KAFKA_CONF=$KAFKA_CONF,topic=$TOPIC,client_id=$CLIENT_ID

echo "========== running DRPC =========="
run_producer
run_benchmark
kill_producer

