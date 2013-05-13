REGISTER /home/hadoop/bigdatatp1/pk.jar;
register /home/hadoop/hbase-0.94.6.1/lib/protobuf-java-2.4.0a.jar;
data = LOAD 'pinkElephantTV/input/boxes.json' USING TextLoader() AS (line:chararray);
channel = LOAD 'hbase://channel' USING org.apache.pig.backend.hadoop.hbase.HBaseStorage('info:name', '-loadKey true') AS (number:long, name:chararray);
tupledJson = FOREACH data GENERATE com.bigdata.pinkelephant.udf.JsonSplitter(line) as t;
flatJson = FOREACH tupledJson GENERATE FLATTEN(t);
projected = FOREACH flatJson GENERATE box_id, channel;
channelLess = FILTER projected By channel != '';
grouped = group channelLess BY channel;

viewList = FOREACH grouped { box = channelLess.box_id; distinct_boxes = DISTINCT box; GENERATE (int)group as number, COUNT(distinct_boxes) as viewers;}

joined = JOIN viewList by number, channel by number;
projectedJoin = FOREACH join GENERATE name, viewers;
sorted = ORDER projectedJoined BY viewers desc;
top10 = LIMIT sorted 10;

STORE top10 into 'results/top10channels' using PigStorage();

