package org.stepanovdg.mapreduce.task1.writable;

import org.apache.hadoop.io.IntWritable;

/**
 * Created by Dmitriy Stepanov on 17.02.18.
 */
public class DescendingIntWritable extends IntWritable {

  public DescendingIntWritable() {
    super();
  }

  public DescendingIntWritable( int value ) {
    super( value );
  }

  @Override public int compareTo( IntWritable o ) {
    return super.compareTo( o ) * -1;
  }
}
