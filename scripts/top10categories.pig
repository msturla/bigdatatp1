REGISTER /home/hadoop/bigdatatp1/pk.jar;
register /home/hadoop/hbase-0.94.6.1/lib/protobuf-java-2.4.0a.jar;
data = LOAD 'pinkElephantTV/input/boxes.json' USING TextLoader() AS (line:chararray);
channel = LOAD 'hbase://channel' USING org.apache.pig.backend.hadoop.hbase.HBaseStorage('info:categories', '-loadKey true') AS (number:long, categories:chararray);
channel_categories = FOREACH channel GENERATE number, FLATTEN(TOKENIZE(REPLACE(categories,' ','_'))) as category:chararray;

tupledJson = FOREACH data GENERATE com.bigdata.pinkelephant.udf.JsonSplitter(line) as t;
flatJson = FOREACH tupledJson GENERATE FLATTEN(t);
channelLess = FILTER flatJson By channel != '';
projected = FOREACH channelLess GENERATE box_id, (int) channel;
joined = JOIN channel_categories BY number, projected by channel;

grouped = group joined BY category;

viewList = FOREACH grouped { box = joined.box_id; distinct_boxes = DISTINCT box; GENERATE group as cat, COUNT(distinct_boxes) as viewers;}

sorted = ORDER viewList BY viewers desc;
top10 = LIMIT sorted 10;

STORE top10 into 'results/top10categories' using PigStorage();