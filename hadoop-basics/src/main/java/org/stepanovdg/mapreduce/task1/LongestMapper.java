package org.stepanovdg.mapreduce.task1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.stepanovdg.mapreduce.task1.writable.DescendingIntWritable;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Created by Dmitriy Stepanov on 16.02.18.
 */
public class LongestMapper extends Mapper<LongWritable, Text, DescendingIntWritable, Text> {

  private final DescendingIntWritable lengthOut = new DescendingIntWritable( 0 );
  private final Text word = new Text();
  private Integer maxLength = 0;

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
}
