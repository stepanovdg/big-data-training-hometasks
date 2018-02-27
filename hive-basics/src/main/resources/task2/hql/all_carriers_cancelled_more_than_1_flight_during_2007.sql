source ${hiveconf:dir}/task2/hql/hivevar.sql;
USE ${hivevar:db_name};

set hive.vectorized.execution.enabled = true;
set hive.vectorized.execution.reduce.enabled = true;

set hive.cbo.enable=true;
set hive.compute.query.using.stats=true;
set hive.stats.fetch.column.stats=true;
set hive.stats.fetch.partition.stats=true;


-- CREATE TEMPORARY TABLE IF NOT EXISTS CANCELLED_FLIGHTS
-- AS
--   SELECT /*+ MAPJOIN(TAB2) */ f.carrier, count(*) as_cancelled_count, a.city
--   FROM flights f JOIN airports a ON f.origin_iata = a.iata
--   WHERE f.canceled
--   GROUP BY f.carrier, a.city
--   HAVING count(*) > 1;

CREATE TEMPORARY TABLE IF NOT EXISTS CANCELLED_FLIGHTS
AS
  SELECT carrier, count(canceled) as_cancelled_count,  origin_iata
  FROM flights
  WHERE canceled
  GROUP BY carrier, origin_iata
  HAVING count(canceled) > 1;

DESCRIBE CANCELLED_FLIGHTS;

SELECT f.carrier, sum(as_cancelled_count) as_cancelled_sum, collect_set(f.city) as_city
FROM CANCELLED_FLIGHTS f
GROUP BY f.carrier
ORDER BY as_cancelled_sum DESC;

