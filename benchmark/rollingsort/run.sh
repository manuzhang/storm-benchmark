#!/bin/sh
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# global configurations

CUR_DIR=`dirname "$0"`
BASE_DIR=$CUR_DIR/..
. $BASE_DIR/conf/config.sh
. $BASE_DIR/bin/functions.sh

# benchmarks configurations

TOPOLOGY_CLASS=storm.benchmark.benchmarks.RollingSort
TOPOLOGY_NAME=RollingSort

COMPONENT=component
SPOUT_NUM=64
SORT_NUM=64

EMIT_FREQ=300 # 5mins
CHUNK_SIZE=2000000 # 2M
MESSAGE_SIZE=10000 # 10KB

WORKERS=16
ACKERS=16
PENDING=200

TOPOLOGY_CONF=topology.name=$TOPOLOGY_NAME,topology.workers=$WORKERS,topology.acker.executors=$ACKERS,topology.max.spout.pending=$PENDING,$COMPONENT.spout_num=$SPOUT_NUM,$COMPONENT.sort_bolt_num=$SORT_NUM,emit.frequency=$EMIT_FREQ,chunk.size=$CHUNK_SIZE,message.size=$MESSAGE_SIZE,topology.worker.childopts=-Xmx16384m

echo "========== running RollingSort =========="
run_benchmark

