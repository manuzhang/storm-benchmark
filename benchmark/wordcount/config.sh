#!/bin/sh

TOPOLOGY_CLASS=FileReadWordCount
TOPOLOGY_NAME=WordCount

WORKERS=4
ACKERS=$WORKERS
PENDING=200

COMPONENT=topology.component
SPOUT_NUM=4
SPLIT_NUM=8
COUNT_NUM=8

TOPOLOGY_CONF=topology.name=$TOPOLOGY_NAME,topology.workers=$WORKERS,topology.acker.executors=$ACKERS,topology.max.spout.pending=$PENDING,$COMPONENT.spout_num=$SPOUT_NUM,$COMPONENT.split_bolt_num=$SPLIT_NUM,$COMPONENT.count_bolt_num=$COUNT_NUM
