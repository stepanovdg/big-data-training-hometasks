package org.stepanovdg.mapreduce.task3;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Partitioner;
import org.stepanovdg.mapreduce.task3.writable.IntTextPairWritable;

/**
 * Created by Dmitriy Stepanov on 20.02.18.
 */
public class HighBidPartionioner extends Partitioner<IntTextPairWritable, LongWritable> {

  @Override
  public int getPartition( IntTextPairWritable intTextPairWritable, LongWritable longWritable, int numPartitions ) {
    return ( intTextPairWritable.getTwo().hashCode() & Integer.MAX_VALUE ) % numPartitions;
  }
}
