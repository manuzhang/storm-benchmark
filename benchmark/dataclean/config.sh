#!/bin/sh

TOPOLOGY_CLASS=DataClean
TOPOLOGY_NAME=DataClean

PRODUCER_NAME=KafkaPageViewProducer
TOPIC="pageview"
CLIENT_ID="dataclean"

WORKERS=4
ACKERS=$WORKERS
PENDING=200

COMPONENT=topology.component
SPOUT_NUM=4
VIEW_NUM=8
FILTER_NUM=8


TOPOLOGY_CONF=topology.name=$TOPOLOGY_NAME,topology.workers=$WORKERS,topology.acker.executors=$ACKERS,topology.max.spout.pending=$PENDING,$COMPONENT.spout_num=$SPOUT_NUM,$COMPONENT.view_bolt_num=$VIEW_NUM,$COMPONENT.filter_bolt_num=$FILTER_NUM
KAFKA_CONF=$KAFKA_CONF,topic=$TOPIC,client_id=$CLIENT_ID
