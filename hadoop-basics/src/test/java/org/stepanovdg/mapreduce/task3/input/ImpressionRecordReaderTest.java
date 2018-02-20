package org.stepanovdg.mapreduce.task3.input;

import org.apache.hadoop.io.IntWritable;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.stepanovdg.mapreduce.task3.RecordUtilTest.IMPRESSION;

/**
 * Created by Dmitriy Stepanov on 19.02.18.
 */
public class ImpressionRecordReaderTest {

  private ImpressionRecordReader impressionRecordReader;

  @Before
  public void setUp() {
    impressionRecordReader = new ImpressionRecordReader( new IntWritable(), new IntWritable() );
  }

  @Test
  public void findKeyValue() throws IOException, InterruptedException {
    byte[] bytes = IMPRESSION.getBytes();
    impressionRecordReader.findKeyValue( IMPRESSION.getBytes(), bytes.length );
    assertEquals( 234, impressionRecordReader.getCurrentKey().get() );
    assertEquals( 277, impressionRecordReader.getCurrentValue().get() );
  }
}