#!/bin/sh

echo "========== running SOL topology =========="
# configure
DIR=`dirname "$0"`
. "${DIR}/../conf/config.sh"
. "${DIR}/config.sh"


run_benchmark



