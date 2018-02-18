package org.stepanovdg.mapreduce.task2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

import static org.stepanovdg.mapreduce.task2.Constants.OUT_SEPARATOR;
import static org.stepanovdg.mapreduce.task2.Constants.TEMP_SEPARATOR;

/**
 * Created by Dmitriy Stepanov on 18.02.18.
 */
public class LogsReducer extends Reducer<IntWritable, Text, Text, Text> {

  private static final Text out = new Text();
  private static final Text ip = new Text();


  @Override protected void reduce( IntWritable key, Iterable<Text> values, Context context )
    throws IOException, InterruptedException {

    Long total = 0L;
    Long count = 0L;
    for ( Text value : values ) {
      String s = value.toString();
      int off = s.indexOf( TEMP_SEPARATOR );
      total += Long.parseLong( s.substring( 0, off ) );
      count += Long.parseLong( s.substring( off + 1 ) );
    }
    if ( total == 0L ) {
      //In case if some reducer doesn't get any information its output should be omitted
      //No such check in combiner as could have values form other mappings(for average)
      return;
    }
    double average = total.doubleValue() / count;

    //not clear output format for average - 1 digit after comma or what?
    out.set( average + OUT_SEPARATOR + total );
    ip.set( "ip" + key );
    context.write( ip, out );
  }
}
