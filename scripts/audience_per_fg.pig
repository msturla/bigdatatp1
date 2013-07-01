REGISTER pk.jar;
register /home/hadoop/hbase-0.94.6.1/lib/protobuf-java-2.4.0a.jar;
data = LOAD 'pinkElephantTV/input/boxes.json' USING TextLoader() AS (line:chararray);
customer = LOAD 'hbase://customer' USING org.apache.pig.backend.hadoop.hbase.HBaseStorage('info:family_group', '-loadKey true') AS (number:long, family_group:chararray);

tupledJson = FOREACH data GENERATE com.bigdata.pinkelephant.udf.JsonSplitter(line) as t;
flatJson = FOREACH tupledJson GENERATE FLATTEN(t);
projected = FOREACH flatJson GENERATE box_id, channel;
audience = FILTER projected By channel != '';
joined = JOIN audience by box_id, customer by number;
grouped = group joined BY family_group;

result = FOREACH grouped {box = joined.box_id; distinct_boxes = DISTINCT box; GENERATE group as family_group, COUNT(distinct_boxes) as audience_quant;}
STORE top10 into 'results/audience_per_fg' using PigStorage();