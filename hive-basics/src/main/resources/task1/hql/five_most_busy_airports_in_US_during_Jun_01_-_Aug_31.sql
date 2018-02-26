source ${hiveconf:dir}/task1/hql/hivevar.sql;
USE ${hivevar:db_name};

set mapred.reduce.tasks=1;

-- SELECT /*+ MAPJOIN(TAB2) */ a.airport, count(*) as_count
-- FROM flights f JOIN airports a ON 1=1
-- WHERE ( f.month > 5 AND f.month < 9 ) AND (f.dest_iata = a.iata OR f.origin_iata = a.iata)
-- GROUP BY (a.airport)
-- SORT BY as_count DESC LIMIT 5;

-- CREATE TEMPORARY TABLE IF NOT EXISTS SUMMER_FLIGHTS
-- AS SELECT * FROM flights f WHERE f.month > 5 AND f.month < 9;

-- CREATE INDEX IATA ON TABLE airports (iata) AS 'BITMAP';
-- CREATE INDEX SUMMER_FLIGHTS_DEST ON TABLE SUMMER_FLIGHTS (dest_iata) AS 'BITMAP';
-- CREATE INDEX SUMMER_FLIGHTS_ORIG ON TABLE SUMMER_FLIGHTS (origin_iata) AS 'BITMAP'; DO NOT work on tez

-- SELECT a.airport, u.as_sum_count FROM airports a JOIN (
-- SELECT union_result.as_iata asiata, SUM(union_result.as_count) as_sum_count FROM (
-- SELECT origin_iata as_iata, count(*) as_count FROM SUMMER_FLIGHTS f LEFT SEMI JOIN airports a ON f.origin_iata = a.iata GROUP BY (origin_iata)
-- UNION
-- SELECT dest_iata as_iata, count(*) as_count FROM SUMMER_FLIGHTS f LEFT SEMI JOIN airports a ON f.dest_iata = a.iata GROUP BY (dest_iata)
-- ) union_result
-- GROUP BY (union_result.as_iata)
-- SORT BY as_sum_count DESC LIMIT 5) u
-- ON a.iata = u.asiata;

SELECT a.airport, u.as_sum_count FROM airports a JOIN (
  SELECT union_result.as_iata asiata, SUM(union_result.as_count) as_sum_count FROM (
    SELECT origin_iata as_iata, count(*) as_count
      FROM flights f LEFT SEMI JOIN airports a
      ON f.origin_iata = a.iata AND f.month > 5 AND f.month < 9
      GROUP BY (origin_iata)
    UNION
    SELECT dest_iata as_iata, count(*) as_count
      FROM flights f LEFT SEMI JOIN airports a
      ON f.dest_iata = a.iata AND f.month > 5 AND f.month < 9
      GROUP BY (dest_iata)
    ) union_result
  GROUP BY (union_result.as_iata)
  SORT BY as_sum_count DESC LIMIT 5) u
ON a.iata = u.asiata;

DROP TABLE SUMMER_FLIGHTS;