#!/bin/sh

TOPOLOGY_CLASS=RollingSort
TOPOLOGY_NAME=RollingSort

WORKERS=4
ACKERS=$WORKERS
PENDING=200

COMPONENT=topology.component
SPOUT_NUM=4
BOLT_NUM=8

WINDOW_LEN=9
EMIT_FREQ=3

TOPOLOGY_CONF=topology.name=$TOPOLOGY_NAME,topology.workers=$WORKERS,topology.acker.executors=$ACKERS,topology.max.spout.pending=$PENDING,$COMPONENT.spout_num=$SPOUT_NUM,$COMPONENT.bolt_num=$BOLT_NUM,window.length=$WINDOW_LEN,emit.frequency=$EMIT_FREQ
