package org.stepanovdg.mapreduce.task1.writable;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparator;

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

  static {                                        // register this comparator
    WritableComparator.define( DescendingIntWritable.class, new Comparator() );
  }

  /**
   * A Comparator optimized for IntWritable.
   */
  public static class Comparator extends WritableComparator {
    public Comparator() {
      super( DescendingIntWritable.class );
    }

    @Override
    public int compare( byte[] b1, int s1, int l1,
                        byte[] b2, int s2, int l2 ) {
      int thisValue = readInt( b1, s1 );
      int thatValue = readInt( b2, s2 );
      return ( thisValue < thatValue ? 1 : ( thisValue == thatValue ? 0 : -1 ) );
    }
  }
}
