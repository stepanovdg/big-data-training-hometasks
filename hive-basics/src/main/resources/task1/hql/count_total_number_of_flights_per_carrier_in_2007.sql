source ${hiveconf:dir}/task1/hql/hivevar.sql;

USE ${hivevar:db_name};

SELECT carrier, count(*) FROM flights WHERE yearOf=2007 GROUP BY carrier;