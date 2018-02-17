package org.stepanovdg.mapreduce.task1;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.stepanovdg.mapreduce.task1.writable.DescendingIntWritable;

/**
 * Created by Dmitriy Stepanov on 16.02.18.
 */
public class LongestMapper extends Mapper<LongWritable, Text, DescendingIntWritable, Text> {

  private final static DescendingIntWritable lengthOut = new DescendingIntWritable( 0 );
  private Text word = new Text();
  private static Integer maxLength = 0;

  @Override
  protected void map( LongWritable key, Text value, Context context )
    throws IOException, InterruptedException {
    StringTokenizer itr = new StringTokenizer( value.toString() );

    while ( itr.hasMoreTokens() ) {
      String wordTemp = itr.nextToken();
      int length = wordTemp.length();
      if ( maxLength > length ) {
        continue;
      } else if ( maxLength < length ) {
        maxLength = length;
        lengthOut.set( maxLength );
      }
      word.set( wordTemp );
      context.write( lengthOut, word );
    }
  }

  @Override
  protected void cleanup( Context context ) throws IOException, InterruptedException {
    if ( context.getCounter( LongestCounter.MAX_LENGTH_KEY ).getValue() < maxLength ) {
      context.getCounter( LongestCounter.MAX_LENGTH_KEY ).setValue( maxLength );
    }
  }
}
