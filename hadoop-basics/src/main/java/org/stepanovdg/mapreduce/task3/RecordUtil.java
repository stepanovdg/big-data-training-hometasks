package org.stepanovdg.mapreduce.task3;

import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.stepanovdg.mapreduce.task3.Constants.*;

/**
 * Created by Dmitriy Stepanov on 19.02.18.
 */
public class RecordUtil {

  public static final Pattern pattern = Pattern.compile( IMP_REGEX );
  public static final Pattern patternWithNames = Pattern.compile( IMP_REGEX_NAMES );

  public static String parseImpressionLinePattern( String impression ) {
    Matcher matcher = pattern.matcher( impression );
    if ( matcher.find() ) {
      return matcher.group( CITY_ID * 2 - 1 ) + " " + matcher.group( BID * 2 - 1 );
    }
    return null;
  }

  public static String parseImpressionLinePatternName( String impression ) {
    Matcher matcher = patternWithNames.matcher( impression );
    if ( matcher.find() ) {
      return matcher.group( "cityId" ) + " " + matcher.group( "bid" );
    }
    return null;
  }

  public static String parseImpressionSplit( String impression ) {
    //Seems to be the same as pattern just pattern compiles ones but here each call
    String[] columns = impression.split( "\t" );
    return columns[ CITY_ID - 1 ] + " " + columns[ BID - 1 ];
  }

  public static String parseImpressionLineIndexOf( String impression ) {
    char tab = '\t';
    int i = impression.lastIndexOf( tab );
    int prev_i = impression.length();
    for ( int tabs = 23; tabs >= 20; tabs-- ) {
      prev_i = i;
      i = impression.lastIndexOf( tab, i - 1 );
    }
    String bid = impression.substring( i + 1, prev_i );

    i = impression.indexOf( tab );
    prev_i = 0;
    for ( int tabs = 1; tabs <= 7; tabs++ ) {
      prev_i = i;
      i = impression.indexOf( tab, i + 1 );
    }
    String city = impression.substring( prev_i + 1, i );
    return city + " " + bid;

  }

  public static String parseImpressionLineTokenaizer( String impression ) {
    StringTokenizer stringTokenizer = new StringTokenizer( impression, "\t" );
    int i = 1;
    StringBuilder sb = new StringBuilder();
    while ( stringTokenizer.hasMoreTokens() ) {
      String s = stringTokenizer.nextToken();
      if ( i == CITY_ID ) {
        sb.append( s ).append( " " );
      }
      if ( i == BID ) {
        sb.append( s );
        return sb.toString();
      }
      i++;
    }
    return null;

  }

  /**
   * Maybe incorrect benchmarking but
   * The fastest:
   * - .indexOf
   * - StringTokenizer
   * - .split
   * - Pattern with group numbers
   * - Pattern with group names
   */
  public static String parseImpressionLine( String impression ) {
    return parseImpressionLineIndexOf( impression );
  }
}
