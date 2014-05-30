#!/bin/bash

echo "========== running KafkaFileReadProducer =========="
CUR_DIR=`dirname "$0"`
BIN_DIR=$CUR_DIR/../../bin
CONF_DIR=$CUR_DIR/../../conf
. $CONF_DIR/config.sh
. $CONF_DIR/kafka_config.sh
. $CUR_DIR/config.sh
. $BIN_DIR/functions.sh

check
run

