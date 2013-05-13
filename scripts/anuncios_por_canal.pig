register /home/hadoop/hbase-0.94.6.1/lib/protobuf-java-2.4.0a.jar;
REGISTER /home/hadoop/bigdatatp1/pk.jar;

day_parts = LOAD 'hbase://day_parts' USING org.apache.pig.backend.hadoop.hbase.HBaseStorage('info:start_timestamp info:end_timestamp info:channel_number info:title info:type info:categories info:description', '-loadKey true') AS ( row:long, start_timestamp:long, end_timestamp:long, channel_number:long, title:chararray, type:chararray, categories:chararray, description:chararray);


ads = FILTER day_parts by type=='ads';
ads_by_channel = GROUP ads BY channel_number;
ads_per_channel = FOREACH ads_by_channel GENERATE group, COUNT(ads.channel_number);

STORE result into 'results/ads_per_channel' using PigStorage();