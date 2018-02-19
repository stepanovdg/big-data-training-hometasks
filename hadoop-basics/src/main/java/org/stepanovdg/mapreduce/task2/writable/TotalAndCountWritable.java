package org.stepanovdg.mapreduce.task2.writable;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by Dmitriy Stepanov on 18.02.18.
 */
public class TotalAndCountWritable implements Writable {

  long bytes;
  long count;

  public TotalAndCountWritable() {
  }

  public long getBytes() {
    return bytes;
  }

  public void setBytes( String bytes ) {
    setBytes( Long.parseLong( bytes ) );
  }

  public void setBytes( long bytes ) {
    this.bytes = bytes;
  }

  public long getCount() {
    return count;
  }

  public void setCount( long count ) {
    this.count = count;
  }

  @Override public void write( DataOutput out ) throws IOException {
    out.writeLong( bytes );
    out.writeLong( count );
  }

  @Override public void readFields( DataInput in ) throws IOException {
    bytes = in.readLong();
    count = in.readLong();
  }

  @Override public int hashCode() {
    return (int) ( bytes * count );
  }

  @Override public boolean equals( Object obj ) {
    return super.equals( obj );
  }

  @Override public String toString() {
    return bytes + " : " + count;
  }
}
