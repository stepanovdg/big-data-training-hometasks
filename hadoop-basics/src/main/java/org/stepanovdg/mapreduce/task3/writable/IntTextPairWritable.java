package org.stepanovdg.mapreduce.task3.writable;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;

/**
 * Created by Dmitriy Stepanov on 20.02.18.
 */
public class IntTextPairWritable extends PairWritable<IntWritable, Text> {

  static {
    // register Only one comparator
    //    WritableComparator.define( IntTextPairWritable.class, new OnlyOneComparator() );

    //register normal comparator
    //iF we need grouping by os type
    WritableComparator.define( IntTextPairWritable.class, getComparator() );
  }

  public IntTextPairWritable() {
    super( new IntWritable(), new Text() );
  }

  @VisibleForTesting
  public IntTextPairWritable( int i, String str ) {
    this();
    setInt( i );
    setText( str );
  }

  //Normal comparator
  private static WritableComparator getComparator() {
    return new WritableComparator() {
      @Override public int compare( byte[] b1, int s1, int l1, byte[] b2, int s2, int l2 ) {
        Integer thisOne = readInt( b1, s1 );
        Integer thatOne = readInt( b2, s2 );
        int compare = thisOne.compareTo( thatOne );
        if ( compare == 0 ) {
          return compareBytes( b1, s1 + 4, l1 - 4, b2, s2 + 4, l2 - 4 );
        } else {
          return compare;
        }
      }
    };
  }

  public void setInt( int value ) {
    one.set( value );
  }

  public void setText( String value ) {
    two.set( value );
  }

  public void setText( byte[] value ) {
    two.set( value );
  }

  public void setText( byte[] value, int start, int len ) {
    two.set( value, start, len );
  }

  @Override public int hashCode() {
    return one.hashCode();
  }

  @Override public boolean equals( Object obj ) {
    return obj != null && obj instanceof PairWritable && ( (PairWritable) obj ).one.equals( one );
  }

  @Override public int compareTo( PairWritable o ) {
    if ( o instanceof IntTextPairWritable ) {
      return one.compareTo( ( (IntTextPairWritable) o ).one );
    }
    return 0;
  }

  @Override protected String toStringSeparator() {
    //may be get from constructor and coonfiguration
    return " ";
  }

  //Only one comparator
  public static class OnlyOneComparator extends WritableComparator {

    public OnlyOneComparator() {
      super( IntTextPairWritable.class );
    }

    @Override
    public int compare( byte[] b1, int s1, int l1,
                        byte[] b2, int s2, int l2 ) {

      Integer thisOne = readInt( b1, s1 );
      Integer thatOne = readInt( b2, s2 );
      return thisOne.compareTo( thatOne );
    }

  }
}
