package org.stepanovdg.mapreduce.task2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.stepanovdg.mapreduce.task2.writable.TotalAndCountWritable;

import java.io.IOException;

/**
 * Created by Dmitriy Stepanov on 18.02.18.
 */
public class LogsCombinerCustom
  extends Reducer<IntWritable, TotalAndCountWritable, IntWritable, TotalAndCountWritable> {

  private static final TotalAndCountWritable out = new TotalAndCountWritable();

  @Override protected void reduce( IntWritable key, Iterable<TotalAndCountWritable> values, Context context )
    throws IOException, InterruptedException {
    Long total = 0L;
    Long count = 0L;
    for ( TotalAndCountWritable value : values ) {
      total += value.getBytes();
      count += value.getCount();
    }
    out.setBytes( total );
    out.setCount( count );
    context.write( key, out );
  }
}
