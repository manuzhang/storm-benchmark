#!/bin/sh

TOPOLOGY_CLASS=KafkaPageViewProducer
TOPOLOGY_NAME=KafkaPageViewProducer

TOPIC="reach"
WORKERS=4
ACKERS=$WORKERS
PENDING=200

COMPONENT=producer
SPOUT_NUM=4
BOLT_NUM=4

TOPOLOGY_CONF=topology.name=$TOPOLOGY_NAME,topology.workers=$WORKERS,topology.acker.executors=$ACKERS,topology.max.spout.pending=$PENDING,$COMPONENT.spout_num=$SPOUT_NUM,$COMPONENT.bolt_num=$BOLT_NUM
KAFKA_CONF=$KAFKA_CONF,topic=$TOPIC
