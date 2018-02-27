source ${hiveconf:dir}/task2/hql/hivevar.sql;
set hivevar:cur_date=${current_date};
SET hive.exec.dynamic.partition = true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.vectorized.execution.enabled = true;
set hive.vectorized.execution.reduce.enabled = true;

CREATE DATABASE IF NOT EXISTS ${hivevar:db_name}
COMMENT 'Hive database home tasks'
LOCATION '${hivevar:db_location}'
WITH DBPROPERTIES ('creator'='${hivevar:creator}','date'='${hivevar:cur_date}');

USE ${hivevar:db_name};

CREATE EXTERNAL TABLE IF NOT EXISTS AIRPORTS(
    iata VARCHAR(3),
    airport string,
    city string,
    state VARCHAR(2),
    country string,
    lat DECIMAL(10,8),
    long DECIMAL(11,8)
)
-- PARTINIONED BY (state VARCHAR(2),city string) comment city and state in table definition
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",",
   "quoteChar"     = "\"",
   "escapeChar"    = "\\"
)
STORED AS TEXTFILE
LOCATION '${hivevar:airport_location}'
TBLPROPERTIES ('serialization.null.format' = '', 'skip.header.line.count' = '1');

CREATE EXTERNAL TABLE IF NOT EXISTS CARRIERS(
    code VARCHAR(6),
    description string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",",
   "quoteChar"     = "\"",
   "escapeChar"    = "\\"
)
STORED AS TEXTFILE
LOCATION '${hivevar:carrier_location}'
TBLPROPERTIES ('serialization.null.format' = '', 'skip.header.line.count' = '1');

CREATE TABLE IF NOT EXISTS FLIGHTS(
--     yearOf DECIMAL(4),
--     month DECIMAL(2),
    day_of_month DECIMAL(2),
    day_of_week DECIMAL(1),
    dep_time DECIMAL(4),
    CRS_dep_time DECIMAL(4),
    arr_time DECIMAL(4),
    CRS_arr_time DECIMAL(4),
    carrier VARCHAR(6),
    fligth_number DECIMAL(4),
    tail_number VARCHAR(6),
    elapsed_time DECIMAL(3),
    CRS_elapsed_time DECIMAL(3),
    air_time DECIMAL(3),
    air_delay DECIMAL(3),
    dep_delay DECIMAL(3),
    origin_iata VARCHAR(3),
    dest_iata VARCHAR(3),
    distance DECIMAL(4),
    taxi_in DECIMAL(3),
    taxi_out DECIMAL(3),
    canceled BOOLEAN,
    cancelation_code CHAR(1),
    diverted BOOLEAN,
    carrier_delay DECIMAL(3),
    weather_delay DECIMAL(3),
    NAS_delay DECIMAL(3),
    securtity_delay DECIMAL(3),
    late_aircraft_delay DECIMAL(3)
)
PARTITIONED BY (yearOf DECIMAL(4), month DECIMAL(2) )
-- ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
-- WITH SERDEPROPERTIES (
--    "separatorChar" = ",",
--    "quoteChar"     = "\"",
--    "escapeChar"    = "\\"
-- )
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS ORC
-- , "orc.bloom.filter.columns"="origin_iata,dest_iata"
TBLPROPERTIES ("orc.compress"="ZLIB",'serialization.null.format' = '');

CREATE EXTERNAL TABLE IF NOT EXISTS FLIGHTS_NP(
    yearOf              DECIMAL(4),
    month               DECIMAL(2),
    day_of_month        DECIMAL(2),
    day_of_week         DECIMAL(1),
    dep_time            DECIMAL(4),
    CRS_dep_time        DECIMAL(4),
    arr_time            DECIMAL(4),
    CRS_arr_time        DECIMAL(4),
    carrier             VARCHAR(6),
    fligth_number       DECIMAL(4),
    tail_number         VARCHAR(6),
    elapsed_time        DECIMAL(3),
    CRS_elapsed_time    DECIMAL(3),
    air_time            DECIMAL(3),
    air_delay           DECIMAL(3),
    dep_delay           DECIMAL(3),
    origin_iata         VARCHAR(3),
    dest_iata           VARCHAR(3),
    distance            DECIMAL(4),
    taxi_in             DECIMAL(3),
    taxi_out            DECIMAL(3),
    canceled            BOOLEAN,
    cancelation_code    CHAR(1),
    diverted            BOOLEAN,
    carrier_delay       DECIMAL(3),
    weather_delay       DECIMAL(3),
    NAS_delay           DECIMAL(3),
    securtity_delay     DECIMAL(3),
    late_aircraft_delay DECIMAL(3)
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",",
   "quoteChar"     = "\"",
   "escapeChar"    = "\\"
)
STORED AS TEXTFILE
LOCATION '${hivevar:flights_location}'
TBLPROPERTIES ('serialization.null.format' = '', 'skip.header.line.count' = '1');

-- LOAD DATA INPATH '${hivevar:flights_location}'
-- OVERWRITE INTO TABLE flights
-- PARTITION(yearOf=2007);

set mapred.reduce.tasks=12;
set hive.exec.reducers.max=100;
set hive.optimize.sort.dynamic.partition=true
set optimize.sort.dynamic.partitioning=true;
set hive.exec.max.dynamic.partitions.pernode=100000;
set hive.exec.max.dynamic.partitions=100000;
set hive.exec.max.created.files=100000;
set hive.tez.java.opts="-Xmx1020m";
set hive.tez.container.size = 1020;
set hive.exec.orc.memory.pool = 1.0;
SET HIVE.TEZ.PRINT.EXEC.SUMMARY = TRUE;
set hive.optimize.index.filter=true;

FROM FLIGHTS_NP F
INSERT OVERWRITE TABLE FLIGHTS PARTITION(yearOf=2007,month)
SELECT
day_of_month       ,
day_of_week        ,
dep_time           ,
CRS_dep_time       ,
arr_time           ,
CRS_arr_time       ,
carrier            ,
fligth_number      ,
tail_number        ,
elapsed_time       ,
CRS_elapsed_time   ,
air_time           ,
air_delay          ,
dep_delay          ,
origin_iata        ,
dest_iata          ,
distance           ,
taxi_in            ,
taxi_out           ,
canceled           ,
cancelation_code   ,
diverted           ,
carrier_delay      ,
weather_delay      ,
NAS_delay          ,
securtity_delay    ,
late_aircraft_delay,
month
DISTRIBUTE BY month
SORT BY day_of_month;

DROP TABLE FLIGHTS_NP;

set hive.vectorized.execution.enabled = true;
set hive.vectorized.execution.reduce.enabled = true;

set hive.cbo.enable=true;
set hive.compute.query.using.stats=true;
set hive.stats.fetch.column.stats=true;
set hive.stats.fetch.partition.stats=true;

ANALYZE TABLE flights COMPUTE STATISTICS FOR COLUMNS;
ANALYZE TABLE airports COMPUTE STATISTICS FOR COLUMNS;
ANALYZE TABLE carriers COMPUTE STATISTICS FOR COLUMNS;
