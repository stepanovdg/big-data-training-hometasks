package org.stepanovdg.mapreduce.task3.writable;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by Dmitriy Stepanov on 20.02.18.
 */
public abstract class PairWritable<V1 extends WritableComparable, V2 extends WritableComparable> implements
  WritableComparable<PairWritable> {

  V1 one;
  V2 two;

  PairWritable( V1 one, V2 two ) {
    this.one = one;
    this.two = two;
  }

  public V1 getOne() {
    return one;
  }

  public void setOne( V1 one ) {
    this.one = one;
  }

  public V2 getTwo() {
    return two;
  }

  public void setTwo( V2 two ) {
    this.two = two;
  }

  @Override public void write( DataOutput out ) throws IOException {
    one.write( out );
    two.write( out );
  }

  @Override public void readFields( DataInput in ) throws IOException {
    one.readFields( in );
    two.readFields( in );
  }

  @Override public int hashCode() {
    return one.hashCode() * two.hashCode();
  }

  @Override public boolean equals( Object obj ) {
    if ( obj == null ) {
      return false;
    }
    if ( obj instanceof PairWritable ) {
      boolean equalsOne = ( (PairWritable) obj ).one.equals( one );
      if ( equalsOne ) {
        return ( (PairWritable) obj ).two.equals( two );
      }
    }
    return false;
  }

  @Override public String toString() {
    return one.toString() + toStringSeparator() + two.toString();
  }

  @Override public int compareTo( PairWritable o ) {
    int i = one.compareTo( o.one );
    if ( i == 0 ) {
      return two.compareTo( o.two );
    } else {
      return i;
    }
  }

  protected abstract String toStringSeparator();
}
