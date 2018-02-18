package org.stepanovdg.mapreduce.task2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

import static org.stepanovdg.mapreduce.task2.Constants.TEMP_SEPARATOR;

/**
 * Created by Dmitriy Stepanov on 18.02.18.
 */
public class LogsCombiner extends Reducer<IntWritable, Text, IntWritable, Text> {

  private static final Text value = new Text();

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
    value.set( "" + total + TEMP_SEPARATOR + count );
    context.write( key, value );
  }
}
