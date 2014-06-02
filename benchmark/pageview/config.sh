#!/bin/sh

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


TOPOLOGY_CONF=topology.name=$TOPOLOGY_NAME,topology.workers=$WORKERS,topology.acker.executors=$ACKERS,topology.max.spout.pending=$PENDING,$COMPONENT.spout_num=$SPOUT_NUM,$COMPONENT.view_bolt_num=$VIEW_NUM,$COMPONENT.count_bolt_num=$COUNT_NUM
KAFKA_CONF=$KAFKA_CONF,topic=$TOPIC,client_id=$CLIENT_ID
