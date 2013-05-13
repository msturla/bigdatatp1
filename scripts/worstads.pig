REGISTER pk.jar;
register /home/hadoop/hbase-0.94.6.1/lib/protobuf-java-2.4.0a.jar;
data = LOAD 'pinkElephantTV/input/boxes.json' USING TextLoader() AS (line:chararray);
json = FOREACH data GENERATE (long) com.bigdata.pinkelephant.udf.JsonFieldAccess(line, 'box_id') as box_id, com.bigdata.pinkelephant.udf.JsonFieldAccess(line, 'power', '') as power, com.bigdata.pinkelephant.udf.JsonFieldAccess(line, 'channel', '') as channel,(long) com.bigdata.pinkelephant.udf.JsonFieldAccess(line, 'timestamp') as timestamp;

grouped = group json BY box_id;
orderedGroup = FOREACH grouped {sorted = ORDER json BY timestamp; GENERATE group, sorted;};
numeratedOrderedGroup = FOREACH orderedGroup GENERATE com.bigdata.pinkelephant.udf.BagEnumerator(sorted) as json;

f1 = FOREACH numeratedOrderedGroup GENERATE FLATTEN(json);
f2 = FOREACH numeratedOrderedGroup GENERATE FLATTEN(json);
joined = JOIN f1 by box_id, f2 by box_id;


programs = LOAD 'hbase://day_parts' USING org.apache.pig.backend.hadoop.hbase.HBaseStorage('info:start_timestamp, info:end_timestamp, info:type, info:channel_number, info:title', '') AS (start_timestamp:long, end_timestamp:long, type:chararray, channel_number:int, title:chararray);
ads = filter programs BY type == 'ads';

channelLeavetime = FOREACH joined GENERATE $0 as box_id, $8 as timestamp, $2 as channel, $4 + 1 as order1, $9 as order2;
filteredChannelLeaveTime = FILTER channelLeavetime BY order1 == order2 AND channel != '';
projectedChannelLeaveTime = FOREACH filteredChannelLeaveTime GENERATE box_id, (int) channel as fromChannel, timestamp;
newJoined = join projectedChannelLeaveTime by fromChannel, programs by channel_number;
filteredJoined = filter newJoined by timestamp < end_timestamp AND timestamp > start_timestamp;
projectedJoin = FOREACH filteredJoined GENERATE box_id, title;
groupedJoin = group filteredJoined by title;
finalCount = FOREACH groupedJoin GENERATE group, COUNT(filteredJoined) as numberLeft;
sorted = ORDER finalCount by numberLeft desc;
top10 = limit sorted 10;

STORE result into 'results/worst_ads' using PigStorage();