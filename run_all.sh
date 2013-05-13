#!/bin/bash --

export HBASE_HOME=/home/hadoop/hbase-0.94.6.1
export HBASE_CONF_DIR=/home/hadoop/hbase-0.94.6.1/conf
export PIG_CLASSPATH=”`${HBASE_HOME}/bin/hbase classpath`:$PIG_CLASSPATH”

FILES=scripts/*.pig
for f in $FILES
do
  echo "Processing $f file..."
  pig $f
done
echo "Your queries have been run and their results are stored in hadoop fs."