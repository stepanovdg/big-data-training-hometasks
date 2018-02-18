package org.stepanovdg.mapreduce.task2;

import eu.bitwalker.useragentutils.Browser;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Locale;

/**
 * Created by Dmitriy Stepanov on 18.02.18.
 */
public class MapreduceTest {
  private static Locale aDefault;
  private MapReduceDriver<LongWritable, Text, IntWritable, Text, Text, Text>
    mapReduceDriver;
  private ReduceDriver<IntWritable, Text, Text, Text> reduceDriver;

  @BeforeClass
  public static void setUpClass() {
    aDefault = Locale.getDefault();
    Locale.setDefault( Locale.US );
  }

  @AfterClass
  public static void tearDown() {
    Locale.setDefault( aDefault );
  }

  @Before
  public void setUp() {
    LogsMapper mapper = new LogsMapper();
    LogsReducer reducer = new LogsReducer();
    LogsCombiner combiner = new LogsCombiner();
    mapReduceDriver = new MapReduceDriver<>();
    mapReduceDriver.setMapper( mapper );
    mapReduceDriver.setReducer( reducer );
    mapReduceDriver.setCombiner( combiner );

    reduceDriver = new ReduceDriver<>();
    reduceDriver.setReducer( reducer );

  }

  @Test
  public void counters() throws IOException {
    mapReduceDriver.addInput( new LongWritable( 1 ), new Text( "ip13 - - [24/Apr/2011:04:41:53 -0400] \"GET "
      + "/logs/access_log.3 HTTP/1.1\" 200 4846545 \"-\" \"Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google"
      + ".com/bot.html)\"" ) );
    mapReduceDriver.withCounter( Browser.BOT, 1 );
    mapReduceDriver.withOutput( new Text( "ip13" ), new Text( "4846545.0,4846545" ) );
    mapReduceDriver.runTest();
  }

  @Test
  public void reducerTest() throws IOException {
    ArrayList<Text> texts = new ArrayList<>();
    texts.add( new Text( "3:1" ) );
    reduceDriver.withInput( new IntWritable( 13 ), texts );
    reduceDriver.withOutput( new Text( "ip13" ), new Text( "3.0,3" ) );
    reduceDriver.runTest();
  }

  @Test
  public void reducerTest2() throws IOException {
    ArrayList<Text> texts = new ArrayList<>();
    texts.add( new Text( "3:1" ) );
    texts.add( new Text( "2:1" ) );
    texts.add( new Text( "4:2" ) );
    reduceDriver.withInput( new IntWritable( 10 ), texts );
    reduceDriver.withOutput( new Text( "ip10" ), new Text( "2.25,9" ) );
    reduceDriver.runTest();

  }
}
