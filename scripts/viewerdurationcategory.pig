REGISTER /home/hadoop/bigdatatp1/pk.jar;
register /home/hadoop/hbase-0.94.6.1/lib/protobuf-java-2.4.0a.jar;
REGISTER /home/hadoop/pig-0.11.1/contrib/piggybank/java/piggybank.jar;

DEFINE UnixToISO org.apache.pig.piggybank.evaluation.datetime.convert.UnixToISO();
DEFINE JsonFieldAccess com.bigdata.pinkelephant.udf.JsonFieldAccess();

data = LOAD 'pinkElephantTV/input/boxes.json' USING TextLoader() AS (line:chararray);
json = FOREACH data GENERATE (long) JsonFieldAccess(line, 'box_id') as box_id, JsonFieldAccess(line, 'power', '') as power, JsonFieldAccess(line, 'channel', '') as channel,(long) JsonFieldAccess(line, 'timestamp') as timestamp;

grouped = group json BY box_id;
orderedGroup = FOREACH grouped {sorted = ORDER json BY timestamp; GENERATE group, sorted;};
numeratedOrderedGroup = FOREACH orderedGroup GENERATE com.bigdata.pinkelephant.udf.BagEnumerator(sorted) as json;

f1 = FOREACH numeratedOrderedGroup GENERATE FLATTEN(json);
f2 = FOREACH numeratedOrderedGroup GENERATE FLATTEN(json);
joined = JOIN f1 by box_id, f2 by box_id;

duration = FOREACH joined GENERATE $0 as box_id, $3 as timestamp, $8 - $3 as duration, $2 as strchannel, $4 + 1 as order1, $9 as order2;
filteredDuration = FILTER duration BY order1 == order2 AND strchannel != '';

projectedDuration = FOREACH filteredDuration GENERATE box_id, duration, (long)strchannel as channel, SUBSTRING(UnixToISO(timestamp),0,10) as date;

channel = LOAD 'hbase://channel' USING org.apache.pig.backend.hadoop.hbase.HBaseStorage('info:categories', '-loadKey true') AS (number:long, categories:chararray);
channel_categories = FOREACH channel GENERATE number, FLATTEN(TOKENIZE(REPLACE(categories,' ','_'))) as category:chararray;

newJoined = join channel_categories by number, projectedDuration by channel;

projectedCategories = FOREACH newJoined GENERATE category, box_id, duration, date;

duration_by_date_and_category = GROUP projectedCategories by (date, category);

category_avg = FOREACH duration_by_date_and_category { box = projectedCategories.box_id; distinct_boxes = DISTINCT box; GENERATE group.date, group.category, (SUM(projectedCategories.duration) / COUNT(distinct_boxes)) AS avg;}

result = FOREACH category_avg GENERATE category, date, avg;

STORE result into 'results/avg_duration_by_cat' using PigStorage();