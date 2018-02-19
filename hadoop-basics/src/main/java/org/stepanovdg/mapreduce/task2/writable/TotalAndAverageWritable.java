package org.stepanovdg.mapreduce.task2.writable;

import org.apache.hadoop.io.BinaryComparable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by Dmitriy Stepanov on 18.02.18.
 * <p>
 * That should be only writable and not used for key, but as there is task for using RowComparator
 * and I already used in {@link IpWritable} but appears that it is not required and I still want ot use
 * IntWritable for keys (for string ip`s like ip1 ip2) so let implement bytes comparator here
 * <p>
 * <p>
 * It would be the long version with all bytes
 */
public class TotalAndAverageWritable extends BinaryComparable implements WritableComparable<BinaryComparable> {

  static {                                        // register this comparator
    WritableComparator.define( TotalAndAverageWritable.class, new Comparator() );
  }

  /**
   * Contains:
   * long - bytes sum (max value 9k petabytes which currently should be enough )
   * double - average (but will store as long)
   */
  private byte[] bytes = new byte[ 16 ];

  public TotalAndAverageWritable() {
  }

  public long getSum() {
    return WritableComparator.readLong( bytes, 0 );
  }

  public void setSum( long sum ) {
    bytes[ 0 ] = (byte) ( sum >>> 56 );
    bytes[ 1 ] = (byte) ( sum >>> 48 );
    bytes[ 2 ] = (byte) ( sum >>> 40 );
    bytes[ 3 ] = (byte) ( sum >>> 32 );
    bytes[ 4 ] = (byte) ( sum >>> 24 );
    bytes[ 5 ] = (byte) ( sum >>> 16 );
    bytes[ 6 ] = (byte) ( sum >>> 8 );
    bytes[ 7 ] = (byte) ( sum >>> 0 );
  }

  public double getAverage() {
    return WritableComparator.readDouble( bytes, 8 );
  }

  public void setAverage( double average ) {
    long aver = Double.doubleToLongBits( average );
    bytes[ 8 ] = (byte) ( aver >>> 56 );
    bytes[ 9 ] = (byte) ( aver >>> 48 );
    bytes[ 10 ] = (byte) ( aver >>> 40 );
    bytes[ 11 ] = (byte) ( aver >>> 32 );
    bytes[ 12 ] = (byte) ( aver >>> 24 );
    bytes[ 13 ] = (byte) ( aver >>> 16 );
    bytes[ 14 ] = (byte) ( aver >>> 8 );
    bytes[ 15 ] = (byte) ( aver >>> 0 );
  }

  @Override public String toString() {
    return getAverage() + "," + getSum();
  }

  @Override public int getLength() {
    return 16;
  }

  @Override public byte[] getBytes() {
    return bytes;
  }

  @Override public void write( DataOutput out ) throws IOException {
    //do not care length it is the same
    out.write( bytes );
  }

  @Override public void readFields( DataInput in ) throws IOException {
    in.readFully( bytes );
  }

  public static class Comparator extends WritableComparator {

    public Comparator() {
      super( TotalAndAverageWritable.class );
    }

    @Override
    public int compare( byte[] b1, int s1, int l1,
                        byte[] b2, int s2, int l2 ) {

      return compareBytes( b1, s1, l1, b2, s2, l2 );
    }

  }
}
