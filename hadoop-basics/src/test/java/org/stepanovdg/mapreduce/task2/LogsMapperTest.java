package org.stepanovdg.mapreduce.task2;

import eu.bitwalker.useragentutils.Browser;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;


/**
 * Created by Dmitriy Stepanov on 18.02.18.
 */
public class LogsMapperTest {

  private MapDriver<LongWritable, Text, IntWritable, Text>
    mapDriver;

  @Before
  public void setUp() {
    LogsMapper mapper = new LogsMapper();
    mapDriver = new MapDriver<>();
    mapDriver.setMapper( mapper );

  }

  @Test
  public void map() throws IOException {
    mapDriver.withInput( new LongWritable( 1 ), new Text( "ip13 - - [24/Apr/2011:04:41:53 -0400] \"GET "
      + "/logs/access_log.3 HTTP/1.1\" 200 4846545 \"-\" \"Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google"
      + ".com/bot.html)\"" ) );
    mapDriver.withOutput( new IntWritable( 13 ), new Text( "4846545:1" ) );
    mapDriver.runTest();
  }

  @Test
  public void testCounter() throws IOException {
    mapDriver.withInput( new LongWritable( 1 ), new Text( "ip13 - - [24/Apr/2011:04:41:53 -0400] \"GET "
      + "/logs/access_log.3 HTTP/1.1\" 200 4846545 \"-\" \"Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google"
      + ".com/bot.html)\"" ) );
    mapDriver.withOutput( new IntWritable( 13 ), new Text( "4846545:1" ) );
    mapDriver.withCounter( Browser.BOT, 1 );
    mapDriver.runTest();
  }
}