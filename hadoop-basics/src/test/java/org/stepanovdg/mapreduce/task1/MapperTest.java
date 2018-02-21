package org.stepanovdg.mapreduce.task1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;
import org.stepanovdg.mapreduce.task1.writable.ComplexIntTextWritable;
import org.stepanovdg.mapreduce.task1.writable.DescendingIntWritable;

import java.io.IOException;

/**
 * Created by Dmitriy Stepanov on 16.02.18.
 */
public class MapperTest {

  private static final NullWritable NUL = NullWritable.get();
  private MapDriver<LongWritable, Text, DescendingIntWritable, Text>
    mapDriver;
  private MapDriver<LongWritable, Text, ComplexIntTextWritable, NullWritable> mapDriverComplex;

  @Before
  public void setUp() {
    LongestMapper mapper = new LongestMapper();
    mapDriver = new MapDriver<>();
    mapDriver.setMapper( mapper );
    LongestMapperComplex complex = new LongestMapperComplex();
    mapDriverComplex = new MapDriver<>();
    mapDriverComplex.setMapper( complex );
  }

  @Test
  public void testMapper() throws IOException {
    mapDriver.withInput( new LongWritable( 1 ), new Text( "a bb ccc bbb dd eeee" ) );
    mapDriver.withOutput( new DescendingIntWritable( 1 ), new Text( "a" ) );
    mapDriver.withOutput( new DescendingIntWritable( 2 ), new Text( "bb" ) );
    mapDriver.withOutput( new DescendingIntWritable( 3 ), new Text( "ccc" ) );
    mapDriver.withOutput( new DescendingIntWritable( 3 ), new Text( "bbb" ) );
    mapDriver.withOutput( new DescendingIntWritable( 4 ), new Text( "eeee" ) );
    mapDriver.runTest();
  }

  @Test
  public void testMapper2() throws IOException {
    mapDriver.withInput( new LongWritable( 1 ), new Text( "eeee a bb ccc bbb dd " ) );
    mapDriver.withOutput( new DescendingIntWritable( 4 ), new Text( "eeee" ) );
    mapDriver.runTest();
  }

  @Test
  public void testMapper3() throws IOException {
    mapDriver.withInput( new LongWritable( 1 ), new Text( "eeee dddd a bb ccc bbb dd " ) );
    mapDriver.withOutput( new DescendingIntWritable( 4 ), new Text( "eeee" ) );
    mapDriver.withOutput( new DescendingIntWritable( 4 ), new Text( "dddd" ) );
    mapDriver.runTest();
  }

  @Test
  public void testMapper4() throws IOException {
    mapDriver.withInput( new LongWritable( 1 ), new Text( "eeee eeee a bb ccc bbb dd " ) );
    mapDriver.withOutput( new DescendingIntWritable( 4 ), new Text( "eeee" ) );
    mapDriver.withOutput( new DescendingIntWritable( 4 ), new Text( "eeee" ) );
    mapDriver.runTest();
  }

  @Test
  public void testMapperComplex() throws IOException {
    mapDriverComplex.withInput( new LongWritable( 1 ), new Text( "a bb ccc bbb dd eeee" ) );
    mapDriverComplex.withOutput( new ComplexIntTextWritable( 1, "a" ), NUL );
    mapDriverComplex.withOutput( new ComplexIntTextWritable( 2, "bb" ), NUL );
    mapDriverComplex.withOutput( new ComplexIntTextWritable( 3, "ccc" ), NUL );
    mapDriverComplex.withOutput( new ComplexIntTextWritable( 3, "bbb" ), NUL );
    mapDriverComplex.withOutput( new ComplexIntTextWritable( 4, "eeee" ), NUL );
    mapDriverComplex.runTest();
  }

  @Test
  public void testMapperComplex2() throws IOException {
    mapDriverComplex.withInput( new LongWritable( 1 ), new Text( "eeee a bb ccc bbb dd " ) );
    mapDriverComplex.withOutput( new ComplexIntTextWritable( 4, "eeee" ), NUL );
    mapDriverComplex.runTest();
  }

  @Test
  public void testMapperComplex3() throws IOException {
    mapDriverComplex.withInput( new LongWritable( 1 ), new Text( "eeee dddd a bb ccc bbb dd " ) );
    mapDriverComplex.withOutput( new ComplexIntTextWritable( 4, "eeee" ), NUL );
    mapDriverComplex.withOutput( new ComplexIntTextWritable( 4, "dddd" ), NUL );
    mapDriverComplex.runTest();
  }

  @Test
  public void testMapperComplex4() throws IOException {
    mapDriverComplex.withInput( new LongWritable( 1 ), new Text( "eeee eeee a bb ccc bbb dd " ) );
    mapDriverComplex.withOutput( new ComplexIntTextWritable( 4, "eeee" ), NUL );
    mapDriverComplex.withOutput( new ComplexIntTextWritable( 4, "eeee" ), NUL );
    mapDriverComplex.runTest();
  }

}
