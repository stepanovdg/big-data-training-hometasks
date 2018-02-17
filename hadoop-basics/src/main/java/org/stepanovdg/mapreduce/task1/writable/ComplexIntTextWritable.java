package org.stepanovdg.mapreduce.task1.writable;

import com.sun.istack.internal.NotNull;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by Dmitriy Stepanov on 17.02.18.
 */

public class ComplexIntTextWritable implements WritableComparable<ComplexIntTextWritable> {

  private DescendingIntWritable intWritable;
  private Text text;


  public ComplexIntTextWritable() {
    text = new Text();
    intWritable = new DescendingIntWritable();
  }

  public ComplexIntTextWritable( String string, int intV ) {
    text = new Text( string );
    intWritable = new DescendingIntWritable( intV );

  }

  public void set( String string ) {
    text.set( string );
  }

  public void set( Integer intV ) {
    intWritable.set( intV );
  }

  public void set( String string, int intV ) {
    set( string );
    set( intV );
  }

  public int getInt() {
    return intWritable.get();
  }

  public DescendingIntWritable getIntWritable() {
    return intWritable;
  }

  public String getString() {
    return text.toString();
  }

  public Text getText() {
    return text;
  }

  @Override public String toString() {
    return text.toString() + " " + intWritable.toString();
  }

  @Override public boolean equals( Object o ) {
    return o instanceof ComplexIntTextWritable && text.equals( ( (ComplexIntTextWritable) o ).text ) && intWritable
      .equals( ( (ComplexIntTextWritable) o ).intWritable );
  }

  @Override public int hashCode() {
    return text.hashCode() + intWritable.hashCode();
  }

  public int compareTo( @NotNull ComplexIntTextWritable o ) {
    int intCompareResult = this.intWritable.compareTo( o.intWritable );
    if ( intCompareResult == 0 ) {
      return this.text.compareTo( o.text );
    } else {
      return intCompareResult;
    }
  }

  public void write( DataOutput out ) throws IOException {
    intWritable.write( out );
    text.write( out );
  }

  public void readFields( DataInput in ) throws IOException {
    intWritable.readFields( in );
    text.readFields( in );
  }

  static {                                        // register this comparator
    WritableComparator.define( ComplexIntTextWritable.class, new Comparator() );
  }

  public static class Comparator extends WritableComparator {
    public Comparator() {
      super( ComplexIntTextWritable.class );
    }

    @Override
    public int compare( byte[] b1, int s1, int l1,
                        byte[] b2, int s2, int l2 ) {
      int thisValue = readInt( b1, s1 );
      int thatValue = readInt( b2, s2 );
      return ( thisValue < thatValue ? 1 :
        ( thisValue == thatValue ? WritableComparator.compareBytes( b1, s1 + 4, l1 - 4,
          b2, s2 + 4, l2 - 4 ) : -1 ) );
    }
  }

}
