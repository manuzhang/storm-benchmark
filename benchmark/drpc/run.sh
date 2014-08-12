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
PRODUCER_DIR=$BASE_DIR/producer/kafka/pageview
. $BASE_DIR/conf/config.sh
. $BASE_DIR/bin/functions.sh

# benchmarks configurations

TOPOLOGY_CLASS=storm.benchmark.benchmarks.DRPC
TOPOLOGY_NAME=DRPC

PRODUCER_NAME=PageViewKafkaProducer
TOPIC="pageview"
CLIENT_ID="drpc"

SPOUT_NUM=3
PAGE_NUM=6
VIEW_NUM=9
USER_NUM=4
FOLLOWER_NUM=8

DRPC_SERVER=intelidh-01
DRPC_PORT=3772

TOPOLOGY_CONF=topology.name=$TOPOLOGY_NAME,topology.workers=$WORKERS,topology.acker.executors=$ACKERS,topology.max.spout.pending=$PENDING,drpc.server=$DRPC_SERVER,drpc.port=$DRPC_PORT,component.spout_num=$SPOUT_NUM,component.page_bolt_num=$PAGE_NUM,component.view_bolt_num=$VIEW_NUM,$component.user_bolt_num=$USER_NUM,$component.follower_bolt_num=$FOLLOWER_NUM
KAFKA_CONF=broker.list=$BROKER_LIST,zookeeper.servers=$ZOOKEEPER_SERVERS,kafka.root.path=$KAFKA_ROOT_PATH,topic=$TOPIC,client_id=$CLIENT_ID
echo $KAFKA_CONF

echo "========== running DRPC =========="
run_producer
run_benchmark
kill_producer

