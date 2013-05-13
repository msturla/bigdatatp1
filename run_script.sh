#!/bin/bash

export HBASE_HOME=/home/hadoop/hbase-0.94.6.1
export HBASE_CONF_DIR=/home/hadoop/hbase-0.94.6.1/conf
export PIG_CLASSPATH=”`${HBASE_HOME}/bin/hbase classpath`:$PIG_CLASSPATH”

pig $1;