package org.stepanovdg.mapreduce.task3.input;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Charsets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

import java.io.IOException;

/**
 * Created by Dmitriy Stepanov on 20.02.18.
 */
public abstract class AImpressionRecordReader<K, V> extends RecordReader<K, V> {

  private final LineRecordReader lineRecordReader;
  K key;
  V value;

  @VisibleForTesting AImpressionRecordReader( LineRecordReader lineRecordReader, K key, V value ) {
    this.lineRecordReader = lineRecordReader;
    this.key = key;
    this.value = value;
  }

  AImpressionRecordReader( Configuration configuration ) {
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
    byte[] line;
    int lineLen;
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
    initKey();
    initValue();
    findKeyValue( line, lineLen );
    return true;
  }

  @Override public K getCurrentKey() {
    return key;
  }

  @Override public V getCurrentValue() {
    return value;
  }

  protected abstract void initKey();

  protected abstract void initValue();

  protected abstract void findKeyValue( byte[] line, int lineLen ) throws IOException;

  int findSeparatorFromEnd( byte[] utf, int end,
                            byte sep ) {
    for ( int i = end - 1; i >= 0; i-- ) {
      if ( utf[ i ] == sep ) {
        return i;
      }
    }
    return -1;
  }

  int findSeparatorFromStart( byte[] utf, int start,
                              byte sep ) {
    for ( int i = start; i <= utf.length; i++ ) {
      if ( utf[ i ] == sep ) {
        return i;
      }
    }
    return -1;
  }


  @Override public float getProgress() throws IOException {
    return lineRecordReader.getProgress();
  }

  @Override public void close() throws IOException {
    lineRecordReader.close();
  }
}
