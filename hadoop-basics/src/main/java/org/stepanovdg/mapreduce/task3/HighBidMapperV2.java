package org.stepanovdg.mapreduce.task3;

import eu.bitwalker.useragentutils.OperatingSystem;
import eu.bitwalker.useragentutils.UserAgent;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.stepanovdg.mapreduce.task3.writable.IntTextPairWritable;

import java.io.IOException;

/**
 * Created by Dmitriy Stepanov on 19.02.18.
 */
public class HighBidMapperV2 extends Mapper<IntWritable, IntTextPairWritable, IntTextPairWritable, LongWritable> {

  private final IntTextPairWritable reduceKey = new IntTextPairWritable();
  private final LongWritable one = new LongWritable( 1 );
  private int borderPrice;

  @Override protected void setup( Context context ) {
    try {
      borderPrice = Integer.parseInt( context.getConfiguration().get( Constants.BORDER_BID_PRICE_PROPERTY_NAME, "250"
      ) );
    } catch ( NumberFormatException e ) {
      borderPrice = 250;
    }
  }

  @Override protected void map( IntWritable key, IntTextPairWritable value, Context context )
    throws IOException, InterruptedException {
    if ( value.getOne().get() > borderPrice ) {
      reduceKey.setOne( key );

      Text two = value.getTwo();
      String uaString = two.toString();
      UserAgent userAgent = UserAgent.parseUserAgentString( uaString );
      OperatingSystem operatingSystem = userAgent.getOperatingSystem().getGroup();
      two.set( operatingSystem.getName() );
      reduceKey.setTwo( two );
      context.write( reduceKey, one );
    }
  }
}
