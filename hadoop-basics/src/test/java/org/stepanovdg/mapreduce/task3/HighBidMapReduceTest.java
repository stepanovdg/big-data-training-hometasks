package org.stepanovdg.mapreduce.task3;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * Created by Dmitriy Stepanov on 19.02.18.
 */
public class HighBidMapReduceTest {

  private static Locale aDefault;
  private MapReduceDriver<IntWritable, IntWritable, IntWritable, LongWritable, Text, LongWritable>
    mapReduceDriver;
  private ReduceDriver<IntWritable, LongWritable, Text, LongWritable> reduceDriver;
  private MapDriver<IntWritable, IntWritable, IntWritable, LongWritable> mapDriver;

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
    HighBidMapper mapper = new HighBidMapper();
    HighBidReducer reducer = new HighBidReducer();
    HighBidCombiner combiner = new HighBidCombiner();
    mapReduceDriver = new MapReduceDriver<>();
    mapReduceDriver.setMapper( mapper );
    mapReduceDriver.setReducer( reducer );
    mapReduceDriver.setCombiner( combiner );

    reduceDriver = new ReduceDriver<>();
    reduceDriver.setReducer( reducer );

    mapDriver = new MapDriver<>();
    mapDriver.setMapper( mapper );
  }

  @Test
  public void testMapper() throws IOException {
    mapDriver.withInput( new IntWritable( 22 ), new IntWritable( 251 ) );
    mapDriver.withInput( new IntWritable( 11 ), new IntWritable( 251 ) );
    mapDriver.withInput( new IntWritable( 22 ), new IntWritable( 249 ) );
    mapDriver.withInput( new IntWritable( 11 ), new IntWritable( 252 ) );
    mapDriver.withInput( new IntWritable( 22 ), new IntWritable( 250 ) );
    mapDriver.withOutput( new IntWritable( 22 ), new LongWritable( 1 ) );
    mapDriver.withOutput( new IntWritable( 11 ), new LongWritable( 1 ) );
    mapDriver.withOutput( new IntWritable( 11 ), new LongWritable( 1 ) );
    mapDriver.runTest();
  }

  @Test
  public void testReducer() throws IOException, URISyntaxException {
    reduceDriver.addCacheFile( new URI( "city.en.txt" + "#" + Constants.CITY_DICTIONARY_FILE_NAME_EN ) );
    List<LongWritable> l1 = new ArrayList<>();
    l1.add( new LongWritable( 1 ) );
    l1.add( new LongWritable( 3 ) );
    l1.add( new LongWritable( 1 ) );
    reduceDriver.withInput( new IntWritable( 11 ), l1 );
    l1 = new ArrayList<>();
    l1.add( new LongWritable( 1 ) );
    reduceDriver.withInput( new IntWritable( 22 ), l1 );
    reduceDriver.withOutput( new Text( "Pekin" ), new LongWritable( 5 ) );
    reduceDriver.withOutput( new Text( "Moscow" ), new LongWritable( 1 ) );
    reduceDriver.runTest();

  }

  @Test
  public void testMapReduce() throws IOException, URISyntaxException {
    reduceDriver.addCacheFile( new URI( "city.en.txt" + "#" + Constants.CITY_DICTIONARY_FILE_NAME_EN ) );
    mapReduceDriver.withInput( new IntWritable( 22 ), new IntWritable( 251 ) );
    mapReduceDriver.withInput( new IntWritable( 11 ), new IntWritable( 251 ) );
    mapReduceDriver.withInput( new IntWritable( 22 ), new IntWritable( 249 ) );
    mapReduceDriver.withInput( new IntWritable( 11 ), new IntWritable( 252 ) );
    mapReduceDriver.withInput( new IntWritable( 22 ), new IntWritable( 250 ) );
    mapReduceDriver.withOutput( new Text( "Pekin" ), new LongWritable( 2 ) );
    mapReduceDriver.withOutput( new Text( "Moscow" ), new LongWritable( 1 ) );
    mapReduceDriver.runTest();
  }
}