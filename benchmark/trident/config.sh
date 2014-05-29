#!/bin/sh

TOPOLOGY_CLASS=TridentWordCount
TOPOLOGY_NAME=TridentWordCount

WORKERS=4
ACKERS=$WORKERS
PENDING=200

COMPONENT=topology.component
SPOUT_NUM=4
SPLIT_NUM=8
COUNT_NUM=8
MAX_BATCH_SIZE=100

TOPOLOGY_CONF=topology.name=$TOPOLOGY_NAME,topology.workers=$WORKERS,topology.acker.executors=$ACKERS,topology.max.spout.pending=$PENDING,$COMPONENT.spout_num=$SPOUT_NUM,$COMPONENT.split_bolt_num=$SPLIT_NUM,$COMPONENT.count_bolt_num=$COUNT_NUM,trident.max.batch.size=$MAX_BATCH_SIZE
