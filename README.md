bigdatatp1
==========

This directory must be copied to a datanode.

Once that is done, use:

./setup.sh


You can also invoke

./run_all.sh

to run all scripts sequentially. You may have to give execution permission to these files.

Also

./run_script.sh filename

will run an individual script. If you want to run scripts with pig yourself, you must set:

export HBASE_HOME=/home/hadoop/hbase-0.94.6.1
export HBASE_CONF_DIR=/home/hadoop/hbase-0.94.6.1/conf
export PIG_CLASSPATH=�`${HBASE_HOME}/bin/hbase classpath`:$PIG_CLASSPATH�

manually. This cannot be done from a script since it has no access to the parent shell caller.

Results of the scripts will be uploaded to hadoop fs, in /user/hadoop/results

At the time of writing, you can download the zip by using:

https://dl.dropboxusercontent.com/u/64978912/bigdatatp1.zip

and then:

unzip bigdatatp1.zip
