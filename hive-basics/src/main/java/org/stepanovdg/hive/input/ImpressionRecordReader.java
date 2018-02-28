package org.stepanovdg.hive.input;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.stepanovdg.mapreduce.task3.writable.IntTextPairWritable;

import java.io.IOException;

/**
 * Created by Dmitriy Stepanov on 27.02.18.
 */
public class ImpressionRecordReader extends AImpressionRecordReader<LongWritable, IntTextPairWritable>
  implements org.apache.hadoop.mapred.RecordReader<LongWritable, IntTextPairWritable> {

  ImpressionRecordReader( LineRecordReader lineRecordReader,
                          LongWritable key, IntTextPairWritable value ) {
    super( null, key, value );
    this.key = key;
    this.value = value;
  }

  ImpressionRecordReader( Configuration configuration, InputSplit split ) throws IOException {
    super( configuration, split );
  }

  @Override protected void initKey() {
    if ( key == null ) {
      key = new LongWritable();
    }
  }

  @Override protected void initValue() {
    if ( value == null ) {
      value = new IntTextPairWritable();
    }
  }

  @Override protected void findKeyValue( byte[] line, int lineLen ) throws IOException {
    byte separator = (byte) '\t';
    int i = findSeparatorFromStart( line, 0, separator );
    int prev_i = 0;
    for ( int tabs = 1; tabs < 5; tabs++ ) {
      prev_i = i;
      i = findSeparatorFromStart( line, i + 1, separator );
    }
    int len = i - prev_i - 1;
    value.setText( line, prev_i + 1, len );

    for ( int tabs = 5; tabs < 8; tabs++ ) {
      prev_i = i;
      i = findSeparatorFromStart( line, i + 1, separator );
    }
    len = i - prev_i - 1;
    value.setInt( Integer.parseInt( Text.decode( line, prev_i + 1, len ) ) );
    key.set( lineLen );
  }

}
