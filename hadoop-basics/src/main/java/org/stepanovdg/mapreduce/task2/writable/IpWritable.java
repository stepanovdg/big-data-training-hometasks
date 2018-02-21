package org.stepanovdg.mapreduce.task2.writable;

import org.apache.hadoop.io.BinaryComparable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;

/**
 * Created by Dmitriy Stepanov on 18.02.18.
 * Expects to have real ip in logs ;(
 */
public class IpWritable extends BinaryComparable implements WritableComparable<BinaryComparable> {

  static final int V6_SIZE = 16;
  static final int V4_SIZE = 4;

  static {                                        // register this comparator
    /*
    A OnlyOneComparator optimized for ByteWritable.
   */
    Comparator comparator = new Comparator();
    WritableComparator.define( IpWritable.class, comparator );
  }

  private byte[] bytes = new byte[ 16 ];
  private boolean ipv4;

  public IpWritable() {
  }

  public IpWritable( InetAddress inetAddress ) {
    set( inetAddress );
  }

  public InetAddress get() {
    try {
      return InetAddress.getByAddress( ipv4 ? Arrays.copyOf( bytes, V4_SIZE ) : bytes );
    } catch ( UnknownHostException e ) {
      throw new RuntimeException( "Should not happened" );
    }
  }

  public void set( InetAddress inetAddress ) {
    if ( inetAddress instanceof Inet4Address ) {
      ipv4 = true;
      bytes = Arrays.copyOf( inetAddress.getAddress(), V4_SIZE );
    } else if ( inetAddress instanceof Inet6Address ) {
      ipv4 = false;
      bytes = inetAddress.getAddress();
    } else {
      throw new RuntimeException( "Not recognised inetAddress type" );
    }
  }

  @Override public int getLength() {
    return ipv4 ? V4_SIZE : V6_SIZE;
  }

  @Override public byte[] getBytes() {
    return bytes;
  }

  @Override public void write( DataOutput out ) throws IOException {
    out.writeBoolean( ipv4 );
    out.write( bytes, 0, ipv4 ? V4_SIZE : V6_SIZE );
  }

  @Override public void readFields( DataInput in ) throws IOException {
    boolean ipv4 = in.readBoolean();
    if ( ipv4 ) {
      in.readFully( bytes, 0, V4_SIZE );
    } else {
      in.readFully( bytes, 0, V6_SIZE );
    }
  }

  @Override public int hashCode() {
    return super.hashCode();
  }

  @Override public boolean equals( Object obj ) {
    return super.equals( obj );
  }

  @Override public String toString() {
    return get().toString();
  }

  public static class Comparator extends WritableComparator {

    public Comparator() {
      super( IpWritable.class );
    }

    @Override
    public int compare( byte[] b1, int s1, int l1,
                        byte[] b2, int s2, int l2 ) {
      byte family1 = b1[ s1 ];
      byte family2 = b2[ s2 ];
      if ( family1 != family2 ) {
        return family1 == 1 ? 1 : -1;
      }
      int len;
      if ( family1 == ( (byte) 1 ) ) {
        len = V4_SIZE;
      } else {
        len = V6_SIZE;
      }
      return compareBytes( b1, s1 + 1, len, b2, s2 + 1, len );
    }

  }


}
