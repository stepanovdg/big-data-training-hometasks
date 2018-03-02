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
public class HighBidMapReduceTest {

  private static String OS = System.getProperty( "os.name" ).toLowerCase();
  private static Locale aDefault;
  private MapReduceDriver<IntWritable, IntWritable, IntWritable, LongWritable, Text, LongWritable>
    mapReduceDriver;
  private ReduceDriver<IntWritable, LongWritable, Text, LongWritable> reduceDriver;
  private MapDriver<IntWritable, IntWritable, IntWritable, LongWritable> mapDriver;
  private static File city;

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
    HighBidMapper mapper = new HighBidMapper();
    final HighBidReducer reducer = new HighBidReducer() {
      @Override protected String getFileNameEn( Context context ) throws IOException {
        //noinspection deprecation
        return context.getLocalCacheFiles()[ 0 ].toString();
      }
    };
    HighBidCombiner combiner = new HighBidCombiner();
    mapReduceDriver.setMapper( mapper );
    mapReduceDriver.setReducer( reducer );
    mapReduceDriver.setCombiner( combiner );

    reduceDriver.setReducer( reducer );

    mapDriver.setMapper( mapper );

    String cityPath = city.getAbsolutePath() + "#" + Constants.CITY_DICTIONARY_FILE_NAME_EN;
    if ( !( OS.contains( "nix" ) || OS.contains( "nux" ) || OS.contains( "aix" ) ) ) {
      cityPath = "file:/" + cityPath.replace( "\\", "/" );
    }
    System.out.println( cityPath );
    reduceDriver =
      reduceDriver.withCacheFile( new URI( cityPath ) );
    mapReduceDriver =
      mapReduceDriver.withCacheFile( new URI( cityPath ) );
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
  public void testReducer() throws IOException {
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
  public void testMapReduce() throws IOException {
    // mrunit distributed cache not working
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
