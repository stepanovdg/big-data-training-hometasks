source ${hiveconf:dir}/task3/hql/hivevar.sql;
set hivevar:cur_date=${current_date};
SET hive.exec.dynamic.partition = true;
set hive.exec.parallel=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.vectorized.execution.enabled = true;
set hive.vectorized.execution.reduce.enabled = true;

CREATE DATABASE IF NOT EXISTS ${hivevar:db_name}
COMMENT 'Hive database home tasks'
LOCATION '${hivevar:db_location}'
WITH DBPROPERTIES ('creator'='${hivevar:creator}','date'='${hivevar:cur_date}');

USE ${hivevar:db_name};

-- ADD JAR hdfs:///${hiveconf:dependency_location};
-- ADD JAR hdfs:///${hiveconf:user_agent_location};
-- TODO do not parse variables in add jar ((( would try to hardcore or add to $HADOOP_HOME/share/hadoop/yarn folder on machines
ADD JAR hdfs:///user/stepanov/hadoop-basics-1.0-SNAPSHOT.jar;
ADD JAR hdfs:///user/stepanov/hive-basics-1.0-SNAPSHOT.jar;

CREATE EXTERNAL TABLE IF NOT EXISTS CITY(
    id DECIMAL(3),
    name STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '${hivevar:city_location}';

-- CREATE TABLE IF NOT EXISTS impression_log(
-- city_id INTEGER,
-- user_agent STRING
-- )
-- PARTITIONED BY (date_day DATE )
-- ROW FORMAT DELIMITED
-- FIELDS TERMINATED BY ','
-- STORED AS ORC
-- TBLPROPERTIES ("orc.compress"="NONE",'serialization.null.format' = '');
--
CREATE EXTERNAL TABLE IF NOT EXISTS impr_part(
    bid_id              DECIMAL(4),
    time_stamp               DECIMAL(2),
    log_type        DECIMAL(2),
    ipin_you_id         DECIMAL(1),
    user_agent            DECIMAL(4),
    ip        DECIMAL(4),
    region_id            DECIMAL(4),
    city_id        DECIMAL(4),
    ad_exchange             VARCHAR(6),
    domain_name       DECIMAL(4),
    url         VARCHAR(6),
    anonymous_url        DECIMAL(3),
    ad_slot_id    DECIMAL(3),
    ad_slot_w            DECIMAL(3),
    ad_slot_h           DECIMAL(3),
    ad_slot_visib           DECIMAL(3),
    ad_slot_format         VARCHAR(3),
    ad_slot_floor_price           VARCHAR(3),
    creative_id            DECIMAL(4),
    bid_price             DECIMAL(3),
    pay_price            DECIMAL(3),
    land_url            BOOLEAN,
    ad_id               CHAR(1),
    user_id            DECIMAL(13)
)
PARTITIONED BY (${hivevar:imp_part_name} DATE )
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE;


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
--TODO do not understand why need
ALTER TABLE impression_log ADD PARTITION (${hivevar:imp_part_name}='2013-10-19') LOCATION '/user/stepanov/impr/date_day=2013-10-19';
ALTER TABLE impression_log ADD PARTITION (${hivevar:imp_part_name}='2013-10-20') LOCATION '/user/stepanov/impr/date_day=2013-10-20';
ALTER TABLE impression_log ADD PARTITION (${hivevar:imp_part_name}='2013-10-21') LOCATION '/user/stepanov/impr/date_day=2013-10-21';
ALTER TABLE impression_log ADD PARTITION (${hivevar:imp_part_name}='2013-10-22') LOCATION '/user/stepanov/impr/date_day=2013-10-22';
ALTER TABLE impression_log ADD PARTITION (${hivevar:imp_part_name}='2013-10-23') LOCATION '/user/stepanov/impr/date_day=2013-10-23';
ALTER TABLE impression_log ADD PARTITION (${hivevar:imp_part_name}='2013-10-24') LOCATION '/user/stepanov/impr/date_day=2013-10-24';
ALTER TABLE impression_log ADD PARTITION (${hivevar:imp_part_name}='2013-10-25') LOCATION '/user/stepanov/impr/date_day=2013-10-25';
ALTER TABLE impression_log ADD PARTITION (${hivevar:imp_part_name}='2013-10-26') LOCATION '/user/stepanov/impr/date_day=2013-10-26';
ALTER TABLE impression_log ADD PARTITION (${hivevar:imp_part_name}='2013-10-27') LOCATION '/user/stepanov/impr/date_day=2013-10-27';
--
show partitions impression_log;
