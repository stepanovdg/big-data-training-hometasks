##Homework for aux module.

* To run copy all resource files into workin dir on cluster.
* Change required properties in run.properties file (and flume-conf.properties for flume).
* Run entry-point.sh specifying task.

Tasks:
- Task 1 - Sqoop
- Task 2 - Flume

##Sqoop:
 - Check if required tables exists 
 - If not exists - put data to hdfs and creates tables
 - Truncate staging table
 - Run sqoop job using staging and clear staging flag
 - Checks result with counting rows in staging table  
 
##Flume:
 * Read from TailDir source (really do not understand 
 condition 
 ```
 'Flume: Used work around to read file (for example write data to netcat instead of source type
   exec (tail -f ..))'
  ```
).
    
   ###According documentation: 
   ```
     "Warning The problem with ExecSource and other asynchronous sources is that the source can not guarantee 
     that if there is a failure to put the event into the Channel the client knows about it. In such cases, 
     the data will be lost. As a for instance, one of the most commonly requested features is the 
     tail -F [file]-like use case where an application writes to a log file on disk and Flume tails the file,
     sending each line as an event. While this is possible, there’s an obvious problem; what happens if the 
     channel fills up and Flume can’t send an event? Flume has no way of indicating to the application writing
     the log file that it needs to retain the log or that the event hasn’t been sent, for some reason.
     If this doesn’t make sense, you need only know this: Your application can never guarantee data has been
     received when using a unidirectional asynchronous interface such as ExecSource! As an extension of this 
     warning - and to be completely clear - there is absolutely zero guarantee of event delivery when using this source.
     For stronger reliability guarantees, consider the Spooling Directory Source, Taildir Source or direct integration
     with Flume via the SDK."
   ```
   
 * Used in memory channel
 * Used HDFS sink