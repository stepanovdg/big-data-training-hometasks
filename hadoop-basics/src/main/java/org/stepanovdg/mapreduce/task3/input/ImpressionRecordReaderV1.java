package org.stepanovdg.mapreduce.task3.input;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;


/**
 * Created by Dmitriy Stepanov on 19.02.18.
 */
public class ImpressionRecordReaderV1 extends AImpressionRecordReader<IntWritable, IntWritable> {

  @VisibleForTesting ImpressionRecordReaderV1( IntWritable key, IntWritable value ) {
    super( null, key, value );
    this.key = key;
    this.value = value;
  }

  ImpressionRecordReaderV1( Configuration configuration ) {
    super( configuration );
  }

  @Override protected void initKey() {
    if ( key == null ) {
      key = new IntWritable();
    }
  }

  @Override protected void initValue() {
    if ( value == null ) {
      value = new IntWritable();
    }
  }

  @Override protected void findKeyValue( byte[] line, int lineLen ) throws IOException {
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
    for ( int tabs = 1; tabs < 8; tabs++ ) {
      prev_i = i;
      i = findSeparatorFromStart( line, i + 1, separator );
    }
    len = i - prev_i - 1;
    key.set( Integer.parseInt( Text.decode( line, prev_i + 1, len ) ) );
  }

}
