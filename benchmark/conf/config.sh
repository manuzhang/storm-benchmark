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

# Where is the storm binary
BIN=/usr/lib/storm/bin/storm

# Absolute path to the stom-benchmark-with-dependencies.jar
JAR=/root/storm-benchmark-0.1.0-jar-with-dependencies.jar

# Please don't modify this
MAIN_CLASS=storm.benchmark.tools.Runner

# We will pull the metrics from nimbus periodically. This defines the interval.
METRICS_POLL_INTERVAL=60000 # 60 secs

 # How long will we run for each benchmark.
METRICS_TOTAL_TIME=300000  # 5 mins

# Where we store the metrics reports. The metrics contains the performance and throughput information.
METRICS_PATH=/root/benchmark/reports
METRICS_CONF=metrics.time=$METRICS_TOTAL_TIME,metrics.poll=$METRICS_POLL_INTERVAL,metrics.path=$METRICS_PATH

# The default workers we use for the benchmark.
WORKERS=4

# The default ack tasks we use for the benchmark.
ACKERS=$WORKERS

# The default max.spout.pending(it will override the default storm config) we use for the benchmarks.
PENDING=200

### the kafka configuration
 # Kafka broker list [node1:port1, node2:port2, ...]
BROKER_LIST=intelidh-04:9092
# Zookeeper server list [node1:port1, node2:port2, ...]
ZOOKEEPER_SERVERS=intelidh-04:2181
# the root path you create for Kafka in Zookeeper
KAFKA_ROOT_PATH=/kafka/kafka-cluster-0
