package org.stepanovdg.mapreduce.task1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.junit.Before;
import org.junit.Test;
import org.stepanovdg.mapreduce.task1.writable.ComplexIntTextWritable;
import org.stepanovdg.mapreduce.task1.writable.DescendingIntWritable;

import java.io.IOException;

/**
 * Created by Dmitriy Stepanov on 17.02.18.
 */
public class MapReduceTest {

  private MapReduceDriver<LongWritable, Text, DescendingIntWritable, Text, IntWritable, Text>
    mapReduceDriver;
  private MapReduceDriver<LongWritable, Text, ComplexIntTextWritable, NullWritable, DescendingIntWritable, Text>
    mapReduceDriver1;

  @Before
  public void setUp() {
    LongestMapper mapper = new LongestMapper();
    LongestReducer reducer = new LongestReducer();
    mapReduceDriver = new MapReduceDriver<LongWritable, Text, DescendingIntWritable, Text, IntWritable, Text>();
    mapReduceDriver.setMapper( mapper );
    mapReduceDriver.setReducer( reducer );

    LongestMapperComplex mapperC = new LongestMapperComplex();
    LongestReducerComplex reducerC = new LongestReducerComplex();
    LongestCombinerComplex combinerC = new LongestCombinerComplex();
    mapReduceDriver1 =
      new MapReduceDriver<LongWritable, Text, ComplexIntTextWritable, NullWritable, DescendingIntWritable, Text>();
    mapReduceDriver1.setMapper( mapperC );
    mapReduceDriver1.setCombiner( combinerC );
    mapReduceDriver1.setReducer( reducerC );
  }

  @Test
  public void mapReduceTest() throws IOException {
    mapReduceDriver.withInput( new LongWritable(), new Text(
      "eeee eeee a bb ccc bbb dd" ) );
    mapReduceDriver.withOutput( new IntWritable( 4 ), new Text( "eeee" ) );
    mapReduceDriver.withOutput( new IntWritable( 4 ), new Text( "eeee" ) );
    mapReduceDriver.runTest();
  }

  //@Test
  //Do not work with the same problem like reduce is not able to override mocked context from mapreduce driver
  // Test for version one is provided
  public void mapReduceTestComplex() throws IOException {
    mapReduceDriver1.withInput( new LongWritable(), new Text(
      "eeee eeee a bb ccc bbb dd" ) );
    mapReduceDriver1.withOutput( new DescendingIntWritable( 4 ), new Text( "eeee" ) );
    mapReduceDriver1.runTest();
  }

}
