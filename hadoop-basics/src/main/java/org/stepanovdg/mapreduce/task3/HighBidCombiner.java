package org.stepanovdg.mapreduce.task3;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by Dmitriy Stepanov on 19.02.18.
 */
public class HighBidCombiner extends Reducer<IntWritable, LongWritable, IntWritable, LongWritable> {
  private final LongWritable amount = new LongWritable();

  @Override protected void reduce( IntWritable key, Iterable<LongWritable> values, Context context )
    throws IOException, InterruptedException {
    long am = 0;
    for ( LongWritable value : values ) {
      am += value.get();
    }
    amount.set( am );
    context.write( key, amount );
  }
}
