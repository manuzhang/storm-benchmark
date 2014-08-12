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
PRODUCER_DIR=$BASE_DIR/producer/kafka/fileread
. $BASE_DIR/conf/config.sh
. $BASE_DIR/bin/functions.sh

# benchmarks configuraitons

TOPOLOGY_CLASS=storm.benchmark.benchmarks.TridentWordCount
TOPOLOGY_NAME=TridentWordCount

PRODUCER_NAME=KafkaFileReadProducer
TOPIC="fileread"
CLIENT_ID="trident"

SPOUT_NUM=4
SPLIT_NUM=8
COUNT_NUM=12

TOPOLOGY_CONF=topology.name=$TOPOLOGY_NAME,topology.workers=$WORKERS,topology.acker.executors=$ACKERS,topology.max.spout.pending=$PENDING,component.spout_num=$SPOUT_NUM,component.split_bolt_num=$SPLIT_NUM,component.count_bolt_num=$COUNT_NUM
KAFKA_CONF=broker.list=$BROKER_LIST,zookeeper.servers=$ZOOKEEPER_SERVERS,kafka.root.path=$KAFKA_ROOT_PATH,topic=$TOPIC,client_id=$CLIENT_ID

echo "========== running TridentWordCount =========="
run_producer
#run_benchmark
run_topology
#kill_producer
