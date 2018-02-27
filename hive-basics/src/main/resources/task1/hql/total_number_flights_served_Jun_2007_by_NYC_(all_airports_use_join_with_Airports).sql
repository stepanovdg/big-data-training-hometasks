source ${hiveconf:dir}/task1/hql/hivevar.sql;
USE ${hivevar:db_name};

CREATE TEMPORARY TABLE IF NOT EXISTS NYC_AIRPORTS
AS SELECT * FROM airports WHERE city='New York';

-- SELECT 'NYC', SUM(union_result.as_count) FROM (
-- SELECT count(*) as_count FROM flights f LEFT SEMI JOIN nyc_airports n ON f.origin_iata = n.iata WHERE f.yearOf=2007 AND f.month=6
-- UNION SELECT count(*) as_count FROM flights f LEFT SEMI JOIN nyc_airports n ON f.dest_iata = n.iata WHERE f.yearOf=2007 AND f.month=6
-- ) union_result;

SELECT /*+ MAPJOIN(TAB2) */ 'NYC', count(*)
FROM flights f JOIN nyc_airports n ON 1=1
WHERE f.yearOf=2007 AND f.month=6 AND (f.dest_iata = n.iata OR f.origin_iata = n.iata);

-- TODO think about ROLLUP DRILLDOWN ALSO

DROP TABLE NYC_AIRPORTS;