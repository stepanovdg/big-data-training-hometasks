source ${hiveconf:dir}/task1/hql/hivevar.sql;
USE ${hivevar:db_name};

SELECT c.description, f.as_count FROM ((
SELECT carrier, count(carrier) as_count
FROM flights
GROUP BY carrier
SORT BY as_count DESC
LIMIT 1
) f INNER JOIN carriers c ON f.carrier = c.code);