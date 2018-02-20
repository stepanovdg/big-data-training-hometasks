package org.stepanovdg.mapreduce.task2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.stepanovdg.mapreduce.task2.writable.TotalAndAverageWritable;
import org.stepanovdg.mapreduce.task2.writable.TotalAndCountWritable;

import java.io.IOException;

/**
 * Created by Dmitriy Stepanov on 18.02.18.
 */
public class LogsReducerCustom extends Reducer<IntWritable, TotalAndCountWritable, Text, TotalAndAverageWritable> {

  private final TotalAndAverageWritable out = new TotalAndAverageWritable();
  private final Text ip = new Text();


  @Override protected void reduce( IntWritable key, Iterable<TotalAndCountWritable> values, Context context )
    throws IOException, InterruptedException {

    Long total = 0L;
    Long count = 0L;
    for ( TotalAndCountWritable value : values ) {
      total += value.getBytes();
      count += value.getCount();
    }
    if ( total == 0L ) {
      //In case if some reducer doesn't get any information its output should be omitted
      //No such check in combiner as could have values form other mappings(for average)
      return;
    }
    double average = total.doubleValue() / count;

    //not clear output format for average - 1 digit after comma or what?
    out.setSum( total );
    out.setAverage( average );
    ip.set( "ip" + key );
    context.write( ip, out );
  }
}
