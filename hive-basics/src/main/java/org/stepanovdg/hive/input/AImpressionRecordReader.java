package org.stepanovdg.hive.input;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Charsets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

import java.io.IOException;

/**
 * Created by Dmitriy Stepanov on 20.02.18.
 */
public abstract class AImpressionRecordReader<K, V> extends RecordReader<K, V>
  implements org.apache.hadoop.mapred.RecordReader<K, V> {

  private final LineRecordReader lineRecordReader;
  private final org.apache.hadoop.mapred.LineRecordReader lineRecordReader2;
  protected K key;
  protected V value;
  private Text innerValue;
  private LongWritable dummyKey;

  @VisibleForTesting
  protected AImpressionRecordReader( LineRecordReader lineRecordReader, K key, V value ) {
    this.lineRecordReader = lineRecordReader;
    this.key = key;
    this.value = value;
    lineRecordReader2 = null;
  }

  protected AImpressionRecordReader( Configuration configuration ) {
    String delimiter = configuration.get(
      "textinputformat.record.delimiter" );
    byte[] recordDelimiterBytes = null;
    if ( null != delimiter ) {
      recordDelimiterBytes = delimiter.getBytes( Charsets.UTF_8 );
    }
    lineRecordReader = new LineRecordReader( recordDelimiterBytes );
    lineRecordReader2 = null;
  }

  protected AImpressionRecordReader( Configuration configuration, org.apache.hadoop.mapred.InputSplit split )
    throws IOException {
    String delimiter = configuration.get(
      "textinputformat.record.delimiter" );
    byte[] recordDelimiterBytes = null;
    if ( null != delimiter ) {
      recordDelimiterBytes = delimiter.getBytes( Charsets.UTF_8 );
    }
    lineRecordReader2 =
      new org.apache.hadoop.mapred.LineRecordReader( configuration, (FileSplit) split, recordDelimiterBytes );
    lineRecordReader = null;
    innerValue = lineRecordReader2.createValue();
    dummyKey = lineRecordReader2.createKey();
  }

  @Override public void initialize( InputSplit split, TaskAttemptContext context )
    throws IOException {
    lineRecordReader.initialize( split, context );
  }

  @Override public boolean nextKeyValue() throws IOException {
    byte[] line;
    int lineLen;
    if ( lineRecordReader2 == null ) {
      if ( lineRecordReader.nextKeyValue() ) {
        innerValue = lineRecordReader.getCurrentValue();
        line = innerValue.getBytes();
        lineLen = innerValue.getLength();
      } else {
        return false;
      }
    } else {
      if ( lineRecordReader2.next( dummyKey, innerValue ) ) {
        line = innerValue.getBytes();
        lineLen = innerValue.getLength();
      } else {
        return false;
      }

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

  protected int findSeparatorFromEnd( byte[] utf, int end,
                                      byte sep ) {
    for ( int i = end - 1; i >= 0; i-- ) {
      if ( utf[ i ] == sep ) {
        return i;
      }
    }
    return -1;
  }

  protected int findSeparatorFromStart( byte[] utf, int start,
                                        byte sep ) {
    for ( int i = start; i <= utf.length; i++ ) {
      if ( utf[ i ] == sep ) {
        return i;
      }
    }
    return -1;
  }


  @Override public float getProgress() throws IOException {
    if ( lineRecordReader != null ) {
      return lineRecordReader.getProgress();
    } else {
      return lineRecordReader2.getProgress();
    }
  }

  @Override public void close() throws IOException {
    if ( lineRecordReader != null ) {
      lineRecordReader.close();
    } else {
      lineRecordReader2.close();
    }
  }

  @Override public boolean next( K key, V value ) throws IOException {
    return nextKeyValue();
  }

  @Override public K createKey() {
    initKey();
    return this.key;
  }

  @Override public V createValue() {
    initValue();
    return this.value;
  }

  @Override public long getPos() throws IOException {
    if ( lineRecordReader2 != null ) {
      return lineRecordReader2.getPos();
    }
    return 0;
  }
}
