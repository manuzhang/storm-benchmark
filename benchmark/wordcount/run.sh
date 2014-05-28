#!/bin/bash

echo "========== running WordCount topology =========="
# configure
DIR=`dirname "$0"`
. "${DIR}/../conf/config.sh"
. "${DIR}/config.sh"

run_benchmark

