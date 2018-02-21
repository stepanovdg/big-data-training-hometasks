package org.stepanovdg.mapreduce.task3;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.stepanovdg.mapreduce.task3.writable.IntTextPairWritable;

import java.io.IOException;

/**
 * Created by Dmitriy Stepanov on 19.02.18.
 */
public class HighBidCombinerV2 extends Reducer<IntTextPairWritable, LongWritable, IntTextPairWritable, LongWritable> {
  private final LongWritable amount = new LongWritable();

  @Override protected void reduce( IntTextPairWritable key, Iterable<LongWritable> values, Context context )
    throws IOException, InterruptedException {
    long am = 0;
    for ( LongWritable value : values ) {
      am += value.get();
    }
    amount.set( am );
    context.write( key, amount );
  }
}
