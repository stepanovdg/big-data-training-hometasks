package org.stepanovdg.mapreduce.task3.input;

import org.apache.hadoop.io.IntWritable;
import org.junit.Before;
import org.junit.Test;
import org.stepanovdg.mapreduce.task3.writable.IntTextPairWritable;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.stepanovdg.mapreduce.task3.RecordUtilTest.IMPRESSION;

/**
 * Created by Dmitriy Stepanov on 19.02.18.
 */
public class ImpressionRecordReaderTest {

  private ImpressionRecordReaderV1 impressionRecordReader;
  private ImpressionRecordReaderV2 impressionRecordReader2;

  @Before
  public void setUp() {
    impressionRecordReader = new ImpressionRecordReaderV1( new IntWritable(), new IntWritable() );
    impressionRecordReader2 = new ImpressionRecordReaderV2( new IntWritable(), new IntTextPairWritable() );
  }

  @Test
  public void findKeyValue() throws IOException {
    byte[] bytes = IMPRESSION.getBytes();
    impressionRecordReader.findKeyValue( IMPRESSION.getBytes(), bytes.length );
    assertEquals( 234, impressionRecordReader.getCurrentKey().get() );
    assertEquals( 277, impressionRecordReader.getCurrentValue().get() );
  }

  @Test
  public void findKeyValue2() throws IOException {
    byte[] bytes = IMPRESSION.getBytes();
    impressionRecordReader2.findKeyValue( IMPRESSION.getBytes(), bytes.length );
    assertEquals( 234, impressionRecordReader2.getCurrentKey().get() );
    assertEquals( 277, impressionRecordReader2.getCurrentValue().getOne().get() );
    assertEquals( "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1)",
      impressionRecordReader2.getCurrentValue().getTwo().toString() );
  }
}
