package org.stepanovdg.mapreduce.task2;

import eu.bitwalker.useragentutils.UserAgent;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.stepanovdg.mapreduce.task2.Constants.*;

/**
 * Created by Dmitriy Stepanov on 18.02.18.
 */
public class LogsMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

  private static final Logger logger = Logger.getLogger( LogsMapper.class );

  private final IntWritable ip = new IntWritable();
  private final Text out = new Text();


  private Pattern pattern;
  private Pattern pattern_304;

  @Override protected void setup( Context context ) {
    String reg = context.getConfiguration().get( AGG_LOG_PATTERN_NAME, REGEX );
    pattern = Pattern.compile( reg );
    pattern_304 = Pattern.compile( REGEX_304 );
  }

  @Override protected void map( LongWritable key, Text value, Context context )
    throws IOException, InterruptedException {
    Matcher matcher = pattern.matcher( value.toString() );
    if ( matcher.find() ) {
      processUserAgent( matcher.group( "userAgent" ), context );
      ip.set( Integer.parseInt( matcher.group( "ip" ) ) );
      out.set( new StringBuilder( matcher.group( "bytes" ) ).append( TEMP_SEPARATOR ).append( 1 ).toString() );
      context.write( ip, out );
    } else {
      matcher = pattern_304.matcher( value.toString() );
      if ( matcher.find() ) {
        processUserAgent( matcher.group( "userAgent" ), context );
        logTrace( value.toString() );
      } else {
        logError( value.toString() );
      }

    }
  }

  private void logTrace( String s ) {
    logger.trace( s );
  }

  private void logError( String value ) {
    logger.error( value );
  }

  private void processUserAgent( String field, Context context ) {
    UserAgent userAgent = UserAgent.parseUserAgentString( field );
    context.getCounter( userAgent.getBrowser() ).increment( 1 );
  }
}
