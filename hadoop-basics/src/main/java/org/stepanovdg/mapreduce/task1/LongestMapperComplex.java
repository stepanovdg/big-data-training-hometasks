package org.stepanovdg.mapreduce.task1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;
import org.stepanovdg.mapreduce.task1.writable.ComplexIntTextWritable;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Created by Dmitriy Stepanov on 16.02.18.
 */
public class LongestMapperComplex extends Mapper<LongWritable, Text, ComplexIntTextWritable, Text> {
  private static final Logger logger = Logger.getLogger( LongestMapperComplex.class );

  private static final ComplexIntTextWritable complexOut = new ComplexIntTextWritable( "", 0 );
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
        complexOut.set( maxLength );
      }
      complexOut.set( wordTemp );
      if ( logger.isDebugEnabled() ) {
        logger.debug( "MapreduceWrite " + complexOut );
      }
      context.write( complexOut, complexOut.getText() );
    }
  }

}
