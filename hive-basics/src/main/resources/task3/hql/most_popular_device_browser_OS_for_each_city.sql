source ${hiveconf:dir}/task3/hql/hivevar.sql;
USE ${hivevar:db_name};

set hive.execution.engine=mr;
set mapreduce.map.memory.mb=510;
set mapreduce.map.java.opts=-Xmx510m;
set mapreduce.reduce.memory.mb=510;
set mapreduce.reduce.java.opts=-Xmx510m;
set mapreduce.reduce.tasks=8;
set mapreduce.map.tasks=8;
SET mapreduce.job.reduces=8;
SET hive.exec.reducers.max=8;
SET mapreduce.job.mappers=8;
set mapred.map.memory.mb=510;
set mapred.map.java.opts=-Xmx510m;
set mapred.map.child.java.opts=-Xmx510m -XX:+UseConcMarkSweepGC;
set mapred.reduce.memory.mb=510;
set mapred.reduce.java.opts=-Xmx510m;
set mapred.reduce.child.java.opts=-Xmx510m -XX:+UseConcMarkSweepGC;
set mapred.reduce.tasks=8;


--FOR TEZ memory all is ok
-- set hive.execution.engine=tez;

-- set hive.execution.engine=spark;
DROP FUNCTION IF EXISTS parseUserAgent;

list JARS;
DELETE JARS;
list JARS;
ADD JAR hdfs:///user/stepanov/hadoop-basics-1.0-SNAPSHOT.jar;
ADD JAR hdfs:///user/stepanov/hive-basics-1.1.1-SNAPSHOT.jar;
ADD JAR hdfs:///user/stepanov/UserAgentUtils-1.21.jar;
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

SELECT /*+ MAPJOIN(f,c) */  f.city_id, NVL(c.name, "NOT FOUND"), f.device, f.d_count, f.os_name, f.os_count, f.browser, f.browser_count
FROM (
SELECT a.city_id,
LAG(a.d,2) over w device,
LAG(a.dc,2) over w d_count,
LAG(a.o,1) over w os_name,
LAG(a.oc,1) over w os_count,
a.b browser, a.bc browser_count
FROM (
	SELECT g.city_id,g.gid,
	g.d, g.dc, RANK() OVER (PARTITION BY g.city_id,g.gid ORDER BY g.dc DESC) r1,
	g.o, g.oc, RANK() OVER (PARTITION BY g.city_id,g.gid ORDER BY g.oc DESC) r2,
	g.b, g.bc, RANK() OVER (PARTITION BY g.city_id,g.gid ORDER BY g.bc DESC) r3
	FROM (
	  		SELECT  p.city_id, GROUPING__ID gid,
				    p.as_struct.device d, COUNT(p.as_struct.device) dc,
					p.as_struct.os_name o, COUNT(p.as_struct.os_name) oc,
					p.as_struct.browser b, COUNT(p.as_struct.browser) bc
   			 FROM
   				  (SELECT city_id, parseUserAgent(user_agent) as_struct
     				FROM impression_log) p
    		GROUP BY p.city_id, p.as_struct.device,p.as_struct.os_name,p.as_struct.browser
    		GROUPING SETS ( (p.city_id, p.as_struct.device), (p.city_id, p.as_struct.os_name), (p.city_id, p.as_struct.browser))
		 )g
)a
WHERE a.r1=1 OR a.r2=1 OR a.r3=1
WINDOW w AS (PARTITION BY a.city_id ORDER BY a.gid )
) f LEFT JOIN city c ON f.city_id = c.id
WHERE f.device IS NOT NULL;
