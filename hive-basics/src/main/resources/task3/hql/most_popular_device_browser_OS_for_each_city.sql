source ${hiveconf:dir}/task3/hql/hivevar.sql;
USE ${hivevar:db_name};

DROP FUNCTION IF EXISTS parseUserAgent;

list JARS;
DELETE JARS;
list JARS;
ADD JAR hdfs:///user/stepanov/hadoop-basics-1.0-SNAPSHOT.jar;
ADD JAR hdfs:///user/stepanov/hive-basics-1.1.1-SNAPSHOT.jar;
list JARS;

--HIVE LOCAL
-- ADD JAR ${hiveconf:dir}/${hiveconf:user_agent_jar};
-- CREATE TEMPORARY FUNCTION parseUserAgent AS 'org.stepanovdg.hive.UserAgentUDF';

-- HIVE GLOBAL
DROP FUNCTION IF EXISTS parseUserAgent;
-- device, OS Name, Browser, UA
-- TODO do not parse variables in add jar ((( would try to hardcore or add to $HADOOP_HOME/share/hadoop/yarn folder on machines
-- CREATE FUNCTION parseUserAgent AS 'org.stepanovdg.hive.UserAgentUDF' USING JAR 'hdfs://${hiveconf:user_agent_location}';
CREATE TEMPORARY FUNCTION parseUserAgent AS 'org.stepanovdg.hive.UserAgentUDF' USING JAR 'hdfs:///user/stepanov/hive-basics-1.1.1-SNAPSHOT.jar';
------------------------------------------------------
SELECT *
FROM impression_log
limit 10;

drop TABLE impression_log;

CREATE EXTERNAL TABLE IF NOT EXISTS impression_log(
city_id INT,
user_agent STRING
)
PARTITIONED BY ( ${hivevar:imp_part_name} DATE )
ROW FORMAT SERDE 'org.stepanovdg.hive.ImpressionSerDe'
WITH SERDEPROPERTIES ( "impression.serde.output.separator" = "\t")
STORED AS
  INPUTFORMAT 'org.stepanovdg.hive.input.CityUserAgentInputFormat'
  OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION '${hivevar:impr_location}';

show partitions impression_log;

ALTER TABLE impression_log ADD PARTITION (${hivevar:imp_part_name}='2013-10-19') LOCATION '/user/stepanov/impr/date_day=2013-10-19';
--
-- LOAD DATA INPATH '/user/stepanov/impr/date_day=2013-10-19'
-- OVERWRITE INTO TABLE impression_log
-- PARTITION(date_day='2013-10-19');

----------------------------------------------------------
SELECT city_id, parseUserAgent(user_agent) as_struct
FROM impression_log;

-- SELECT
-- FROM (
  SELECT city_id, parseUserAgent(user_agent) as_struct,as_struct.device,COUNT(as_struct.device),as_struct.os_name,COUNT(as_struct.os_name),as_struct.browser, COUNT(as_struct.browser)
  FROM (
--     SELECT city_id, parseUserAgent(user_agent) as_struct
--     FROM impression_log
    impression_log
  )
  GROUP BY city_id, as_struct.device,as_struct.os_name,as_struct.browser
  GROUPING SETS ( (city_id, as_struct.device), (city_id, as_struct.os_name), (city_id, as_struct.browser))
  HAVING (MAX(COUNT(as_struct.device)) = COUNT(as_struct.device) OR
         MAX(COUNT(as_struct.os_name)) = COUNT(as_struct.os_name) OR
         MAX(COUNT(as_struct.browser)) = COUNT(as_struct.browser));
-- );