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
import org.stepanovdg.mapreduce.task3.writable.IntTextPairWritable;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * Created by Dmitriy Stepanov on 19.02.18.
 */
public class HighBidMapReduceV2Test {

  private static Locale aDefault;
  private static File city;
  private MapReduceDriver<IntWritable, IntTextPairWritable, IntTextPairWritable, LongWritable, Text, LongWritable>
    mapReduceDriver;
  private ReduceDriver<IntTextPairWritable, LongWritable, Text, LongWritable> reduceDriver;
  private MapDriver<IntWritable, IntTextPairWritable, IntTextPairWritable, LongWritable> mapDriver;

  @BeforeClass
  public static void setUpClass() {
    aDefault = Locale.getDefault();
    Locale.setDefault( Locale.US );
  }

  @AfterClass
  public static void tearDown() {
    Locale.setDefault( aDefault );
  }

  @BeforeClass
  public static void setupCityFile() throws IOException {
    city = File.createTempFile( "city", ".txt" );
    PrintWriter printWriter = new PrintWriter( city );
    printWriter.println( "11\tPekin" );
    printWriter.println( "22\tMoscow" );
    printWriter.close();
  }

  @Before
  public void setUp() throws URISyntaxException {
    mapReduceDriver = new MapReduceDriver<>();
    reduceDriver = new ReduceDriver<>();
    mapDriver = new MapDriver<>();
    HighBidMapperV2 mapper = new HighBidMapperV2();
    final HighBidReducerV2 reducer = new HighBidReducerV2() {
      @Override protected String getFileNameEn( Context context ) throws IOException {
        //noinspection deprecation
        return context.getLocalCacheFiles()[ 0 ].toString();
      }
    };
    HighBidCombinerV2 combiner = new HighBidCombinerV2();
    mapReduceDriver.setMapper( mapper );
    mapReduceDriver.setReducer( reducer );
    mapReduceDriver.setCombiner( combiner );

    reduceDriver.setReducer( reducer );

    mapDriver.setMapper( mapper );

    reduceDriver =
      reduceDriver.withCacheFile( new URI( city.getAbsolutePath() + "#" + Constants.CITY_DICTIONARY_FILE_NAME_EN ) );
    mapReduceDriver =
      mapReduceDriver.withCacheFile( new URI( city.getAbsolutePath() + "#" + Constants.CITY_DICTIONARY_FILE_NAME_EN ) );
  }

  @Test
  public void testMapper() throws IOException {
    mapDriver.withInput( new IntWritable( 22 ), new IntTextPairWritable( 251, "agent string1" ) );
    mapDriver.withInput( new IntWritable( 11 ), new IntTextPairWritable( 251, "agent string2" ) );
    mapDriver.withInput( new IntWritable( 22 ), new IntTextPairWritable( 249, "agent string1" ) );
    mapDriver.withInput( new IntWritable( 11 ), new IntTextPairWritable( 252, "agent string1" ) );
    mapDriver.withInput( new IntWritable( 22 ), new IntTextPairWritable( 250, "agent string2" ) );
    mapDriver.withOutput( new IntTextPairWritable( 22, "agent string1" ), new LongWritable( 1 ) );
    mapDriver.withOutput( new IntTextPairWritable( 11, "agent string2" ), new LongWritable( 1 ) );
    mapDriver.withOutput( new IntTextPairWritable( 11, "agent string1" ), new LongWritable( 1 ) );
    mapDriver.runTest();
  }

  @Test
  public void testReducer() throws IOException {
    List<LongWritable> l1 = new ArrayList<>();
    l1.add( new LongWritable( 1 ) );
    l1.add( new LongWritable( 3 ) );
    l1.add( new LongWritable( 1 ) );
    reduceDriver.withInput( new IntTextPairWritable( 11, "ua1" ), l1 );
    l1 = new ArrayList<>();
    l1.add( new LongWritable( 1 ) );
    reduceDriver.withInput( new IntTextPairWritable( 22, "ua2" ), l1 );
    reduceDriver.withOutput( new Text( "Pekin\tua1" ), new LongWritable( 5 ) );
    reduceDriver.withOutput( new Text( "Moscow\tua2" ), new LongWritable( 1 ) );
    reduceDriver.runTest();

  }

  @Test
  public void testMapReduce() throws IOException {
    // mrunit distributed cache not working
    mapReduceDriver.withInput( new IntWritable( 22 ),
      new IntTextPairWritable( 251, "Mozilla/4.0 (compatible; MSIE 6.0; Linux; SV1)" ) );
    mapReduceDriver.withInput( new IntWritable( 11 ),
      new IntTextPairWritable( 251, "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1)" ) );
    mapReduceDriver.withInput( new IntWritable( 22 ),
      new IntTextPairWritable( 249, "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1)" ) );
    mapReduceDriver.withInput( new IntWritable( 11 ),
      new IntTextPairWritable( 252, "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1)" ) );
    mapReduceDriver.withInput( new IntWritable( 22 ),
      new IntTextPairWritable( 250, "Mozilla/4.0 (compatible; MSIE 6.0; Linux; SV1)" ) );
    mapReduceDriver.withOutput( new Text( "Pekin\tWindows" ), new LongWritable( 2 ) );
    mapReduceDriver.withOutput( new Text( "Moscow\tLinux" ), new LongWritable( 1 ) );
    mapReduceDriver.runTest();
  }
}
