#!/bin/sh

TOPOLOGY_CLASS=DRPC
TOPOLOGY_NAME=DRPC

PRODUCER_NAME=KafkaPageViewProducer
TOPIC="reach"
CLIENT_ID="drpc"

WORKERS=4
ACKERS=$WORKERS
PENDING=200

COMPONENT=topology.component
SPOUT_NUM=4
PAGE_NUM=8
VIEW_NUM=12

DRPC_SERVER=intelidh-01
DRPC_PORT=54598

TOPOLOGY_CONF=topology.name=$TOPOLOGY_NAME,topology.workers=$WORKERS,topology.acker.executors=$ACKERS,topology.max.spout.pending=$PENDING,drpc.server=$DRPC_SERVER,drpc.port=$DRPC_PORT,$COMPONENT.spout_num=$SPOUT_NUM,$COMPONENT.page_bolt_num=$PAGE_NUM,$COMPONENT.view_bolt_num=$VIEW_NUM
KAFKA_CONF=$KAFKA_CONF,topic=$TOPIC,client_id=$CLIENT_ID
