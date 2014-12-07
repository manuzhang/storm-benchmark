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

# load global configurations

CUR_DIR=`dirname "$0"`
BASE_DIR=$CUR_DIR/..
. $BASE_DIR/conf/config.sh
. $BASE_DIR/bin/functions.sh

# benchmarks configuration

TOPOLOGY_CLASS=storm.benchmark.benchmarks.SOL
TOPOLOGY_NAME=SOL

TOPOLOGY_LEVEL=3
MESSAGE_SIZE=100

SPOUT_NUM=4
BOLT_NUM=4

TOPOLOGY_CONF=topology.name=$TOPOLOGY_NAME,topology.workers=$WORKERS,topology.acker.executors=$ACKERS,topology.max.spout.pending=$PENDING,benchmarks.level=$TOPOLOGY_LEVEL,component.spout_num=$SPOUT_NUM,component.bolt_num=$BOLT_NUM

echo "========== running SOL =========="
run_benchmark

