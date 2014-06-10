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

BIN=/usr/lib/storm/bin/storm
JAR=/root/storm-benchmark-0.1.0-jar-with-dependencies.jar
MAIN_CLASS=storm.benchmark.tools.Runner

METRICS_POLL_INTERVAL=60000 # 60 secs
METRICS_TOTAL_TIME=300000  # 5 mins
METRICS_PATH=/root/benchmark/reports
METRICS_CONF=metrics.time=$METRICS_TOTAL_TIME,metrics.poll=$METRICS_POLL_INTERVAL,metrics.path=$METRICS_PATH

BROKER_LIST=intelidh-04:9092
ZOOKEEPER_SERVERS=intelidh-04:2181
KAFKA_ROOT_PATH=/kafka/kafka-cluster-0

WORKERS=4
ACKERS=$WORKERS
PENDING=200

