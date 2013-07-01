REGISTER pk.jar;
register /home/hadoop/hbase-0.94.6.1/lib/protobuf-java-2.4.0a.jar;
data = LOAD 'pinkElephantTV/input/boxes.json' USING TextLoader() AS (line:chararray);
customer = LOAD 'hbase://customer' USING org.apache.pig.backend.hadoop.hbase.HBaseStorage('info:client_type', '-loadKey true') AS (number:long, client_type:chararray);

tupledJson = FOREACH data GENERATE com.bigdata.pinkelephant.udf.JsonSplitter(line) as t;
flatJson = FOREACH tupledJson GENERATE FLATTEN(t);
projected = FOREACH flatJson GENERATE box_id, channel;
audience = FILTER projected By channel != '';
joined = JOIN audience by box_id, customer by number;
grouped = group joined BY client_type;

result = FOREACH grouped {box = joined.box_id; distinct_boxes = DISTINCT box;  GENERATE group as client_type, COUNT(distinct_boxes) as audience_quant;}
STORE result into 'results/audience_per_type' using PigStorage();