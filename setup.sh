#!/bin/bash

cd pinkelephant
mvn package
hadoop fs -put target/pinkelephant-0.0.1-SNAPSHOT.jar pk.jar
cd ..
export HBASE_HOME=/home/hadoop/hbase-0.94.6.1
export HBASE_CONF_DIR=/home/hadoop/hbase-0.94.6.1/conf
export PIG_CLASSPATH=”`${HBASE_HOME}/bin/hbase classpath`:$PIG_CLASSPATH”

echo "You can now run your pig queries!"