package org.stepanovdg.mapreduce.task3;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by Dmitriy Stepanov on 19.02.18.
 */
public class HighBidMapper extends Mapper<IntWritable, IntWritable, IntWritable, LongWritable> {

  public final LongWritable one = new LongWritable( 1 );
  private int borderPrice;

  @Override protected void setup( Context context ) {
    try {
      borderPrice = Integer.parseInt( context.getConfiguration().get( Constants.BORDER_BID_PRICE_PROPERTY_NAME, "250"
      ) );
    } catch ( NumberFormatException e ) {
      borderPrice = 250;
    }
  }

  @Override protected void map( IntWritable key, IntWritable value, Context context )
    throws IOException, InterruptedException {
    if ( value.get() > borderPrice ) {
      context.write( key, one );
    }
  }
}
