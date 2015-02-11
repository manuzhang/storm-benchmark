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

run_benchmark() {
  check
  run_topology
  kill_topology
}


check() {
  if [ ! -e $JAR ]; then
    echo "JAR file not found; exit..."
    exit -1
  fi
}

run_topology() {
  OS="`uname`"
  SED_OPT="-r"
  case $OS in
    'Darwin') 
      SED_OPT="-E"
      ;;
    *) ;;
  esac
  if [ -n "$KAFKA_CONF" ]; then
    CONFIG=$TOPOLOGY_CONF,$METRICS_CONF,$KAFKA_CONF
  else
    CONFIG=$TOPOLOGY_CONF,$METRICS_CONF
  fi
  CONFIG=`echo $CONFIG | sed $SED_OPT "s/,/ -c /g"`
  echo $CONFIG
  $BIN jar $JAR $MAIN_CLASS $TOPOLOGY_CLASS -c $CONFIG
}


kill_topology() {
  $BIN kill $TOPOLOGY_NAME
}

run_producer() {
  sh $PRODUCER_DIR/run.sh
}

kill_producer() {
  $BIN kill $PRODUCER_NAME
}

