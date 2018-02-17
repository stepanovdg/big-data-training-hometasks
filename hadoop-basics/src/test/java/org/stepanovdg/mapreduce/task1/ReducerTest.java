package org.stepanovdg.mapreduce.task1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.stepanovdg.mapreduce.task1.writable.ComplexIntTextWritable;
import org.stepanovdg.mapreduce.task1.writable.DescendingIntWritable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by Dmitriy Stepanov on 16.02.18.
 */
public class ReducerTest {

  private ReduceDriver<ComplexIntTextWritable, NullWritable, DescendingIntWritable, Text>
    reduceDriverComplex;
  private ReduceDriver<DescendingIntWritable, Text, IntWritable, Text> reduceDriver;

  @Before
  public void setUp() {
    LongestReducer reducer = new LongestReducer();
    reduceDriver = new ReduceDriver<DescendingIntWritable, Text, IntWritable, Text>();
    reduceDriver.setReducer( reducer );
    LongestReducerComplex reducerComplex = new LongestReducerComplex();
    reduceDriverComplex = new ReduceDriver<ComplexIntTextWritable, NullWritable, DescendingIntWritable, Text>();
    reduceDriverComplex.setReducer( reducerComplex );
  }

  @Test
  public void testReducer() throws IOException {
    ArrayList<Text> texts = new ArrayList<Text>();
    Text ccc = new Text( "ccc" );
    texts.add( ccc );
    texts.add( ccc );
    Text bbb = new Text( "bbb" );
    texts.add( bbb );
    Text aaa = new Text( "aaa" );
    texts.add( aaa );
    reduceDriver.withInput( new DescendingIntWritable( 3 ), texts );
    texts = new ArrayList<Text>();
    texts.add( new Text( "cc" ) );
    texts.add( new Text( "bb" ) );
    texts.add( new Text( "aa" ) );
    reduceDriver.withInput( new DescendingIntWritable( 2 ), texts );
    IntWritable intWritable = new IntWritable( 3 );
    reduceDriver.withOutput( intWritable, ccc );
    reduceDriver.withOutput( intWritable, ccc );
    reduceDriver.withOutput( intWritable, bbb );
    reduceDriver.withOutput( intWritable, aaa );
    reduceDriver.runTest();

  }

  @Test
  public void testReducerComplex() throws Exception {
    final ArrayList<NullWritable> nullWritables = new ArrayList<NullWritable>();
    nullWritables.add( NullWritable.get() );
    final ArrayList<NullWritable> nullWritablesDouble = new ArrayList<NullWritable>();
    nullWritables.add( NullWritable.get() );
    nullWritables.add( NullWritable.get() );
    Text ccc = new Text( "ccc" );
    Text bbb = new Text( "bbb" );
    Text aaa = new Text( "aaa" );

    final LinkedHashMap<ComplexIntTextWritable, Iterable<NullWritable>> answers =
      new LinkedHashMap<ComplexIntTextWritable, Iterable<NullWritable>>();
    answers.put( null, null );
    ComplexIntTextWritable ccc1 = new ComplexIntTextWritable( 3, "ccc" );
    ComplexIntTextWritable bbb1 = new ComplexIntTextWritable( 3, "bbb" );
    ComplexIntTextWritable aaa1 = new ComplexIntTextWritable( 3, "aaa" );
    ComplexIntTextWritable aa = new ComplexIntTextWritable( 2, "aa" );
    ComplexIntTextWritable bb = new ComplexIntTextWritable( 2, "bb" );
    ComplexIntTextWritable cc = new ComplexIntTextWritable( 2, "cc" );
    answers.put( ccc1, nullWritablesDouble );
    answers.put( bbb1, nullWritables );
    answers.put( aaa1, nullWritables );
    answers.put( aa, nullWritables );
    answers.put( bb, nullWritables );
    answers.put( cc, nullWritables );
    reduceDriverComplex.withInput( ccc1, nullWritables );
    reduceDriverComplex.withInput( ccc1, nullWritables );
    reduceDriverComplex.withInput( bbb1, nullWritables );
    reduceDriverComplex.withInput( aaa1, nullWritables );
    reduceDriverComplex.withInput( aa, nullWritables );
    reduceDriverComplex.withInput( bb, nullWritables );
    reduceDriverComplex.withInput( cc, nullWritables );
    DescendingIntWritable intWritable = new DescendingIntWritable( 3 );
    reduceDriverComplex.withOutput( intWritable, ccc );
    reduceDriverComplex.withOutput( intWritable, bbb );
    reduceDriverComplex.withOutput( intWritable, aaa );

    //Issue with mrunit nullpointer needed in fact to add check for null first run - use hardcode to avoid
    // null pointer in org.apache.hadoop.mrunit.internal.mapreduce.MockReduceContextWrapper 55 line
    final AtomicInteger counter = new AtomicInteger( 0 );
    final Reducer.Context mockitoContext = reduceDriverComplex.getContext();
    Mockito.doAnswer( new Answer<ComplexIntTextWritable>() {
      public ComplexIntTextWritable answer( InvocationOnMock invocation ) {
        return answers.keySet().toArray( new ComplexIntTextWritable[] {} )[ counter.get() ];
      }
    } ).when( mockitoContext ).getCurrentKey();
    Mockito.doAnswer( new Answer<Boolean>() {
      public Boolean answer( InvocationOnMock invocation ) {
        return answers.size() > counter.incrementAndGet();
      }
    } ).when( mockitoContext ).nextKey();
    Mockito.doAnswer( new Answer<Iterable<NullWritable>>() {
      public Iterable<NullWritable> answer( InvocationOnMock invocation ) throws IOException {
        if ( counter.get() == 0 ) {
          throw new IOException( "tries to work with values without callin nextkey" );
        } else {
          ComplexIntTextWritable key =
            answers.keySet().toArray( new ComplexIntTextWritable[] {} )[ counter.get() ];
          return answers.get( key );
        }
      }
    } ).when( mockitoContext ).getValues();
    // end workaround

    reduceDriverComplex.runTest();

  }
}