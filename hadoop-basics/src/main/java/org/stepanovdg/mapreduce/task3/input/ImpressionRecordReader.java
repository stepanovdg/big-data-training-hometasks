package org.stepanovdg.mapreduce.task3.input;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Charsets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

import java.io.IOException;


/**
 * Created by Dmitriy Stepanov on 19.02.18.
 */
public class ImpressionRecordReader extends RecordReader<IntWritable, IntWritable> {

  private final LineRecordReader lineRecordReader;
  private IntWritable key;
  private IntWritable value;

  @VisibleForTesting ImpressionRecordReader( IntWritable key, IntWritable value ) {
    this.key = key;
    this.value = value;
    lineRecordReader = null;
  }

  public ImpressionRecordReader( Configuration configuration ) {
    String delimiter = configuration.get(
      "textinputformat.record.delimiter" );
    byte[] recordDelimiterBytes = null;
    if ( null != delimiter ) {
      recordDelimiterBytes = delimiter.getBytes( Charsets.UTF_8 );
    }
    lineRecordReader = new LineRecordReader( recordDelimiterBytes );

  }

  @Override public void initialize( InputSplit split, TaskAttemptContext context )
    throws IOException {
    lineRecordReader.initialize( split, context );
  }

  @Override public boolean nextKeyValue() throws IOException {
    byte[] line = null;
    int lineLen = -1;
    if ( lineRecordReader.nextKeyValue() ) {
      Text innerValue = lineRecordReader.getCurrentValue();
      line = innerValue.getBytes();
      lineLen = innerValue.getLength();
    } else {
      return false;
    }
    if ( line == null ) {
      return false;
    }
    if ( key == null ) {
      key = new IntWritable();
    }
    if ( value == null ) {
      value = new IntWritable();
    }
    findKeyValue( line, lineLen );
    return true;
  }

  private int findSeparatorFromEnd( byte[] utf, int end,
                                    byte sep ) {
    for ( int i = end - 1; i >= 0; i-- ) {
      if ( utf[ i ] == sep ) {
        return i;
      }
    }
    return -1;
  }

  private int findSeparatorFromStart( byte[] utf, int start,
                                      byte sep ) {
    for ( int i = start; i <= utf.length; i++ ) {
      if ( utf[ i ] == sep ) {
        return i;
      }
    }
    return -1;
  }

  @VisibleForTesting
  void findKeyValue( byte[] line, int lineLen ) throws IOException {
    byte separator = (byte) '\t';
    int i = findSeparatorFromEnd( line, lineLen, separator );
    int prev_i = lineLen;
    for ( int tabs = 23; tabs >= 20; tabs-- ) {
      prev_i = i;
      i = findSeparatorFromEnd( line, i - 1, separator );
    }

    int len = prev_i - i - 1;
    value.set( Integer.parseInt( Text.decode( line, i + 1, len ) ) );

    i = findSeparatorFromStart( line, 0, separator );
    prev_i = 0;
    for ( int tabs = 1; tabs <= 7; tabs++ ) {
      prev_i = i;
      i = findSeparatorFromStart( line, i + 1, separator );
    }

    len = i - prev_i - 1;
    key.set( Integer.parseInt( Text.decode( line, prev_i + 1, len ) ) );
  }

  @Override public IntWritable getCurrentKey() {
    return key;
  }

  @Override public IntWritable getCurrentValue() {
    return value;
  }

  @Override public float getProgress() throws IOException {
    return lineRecordReader.getProgress();
  }

  @Override public void close() throws IOException {
    lineRecordReader.close();
  }
}
