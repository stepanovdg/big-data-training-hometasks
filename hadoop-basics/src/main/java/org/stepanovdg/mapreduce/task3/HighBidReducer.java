package org.stepanovdg.mapreduce.task3;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

/**
 * Created by Dmitriy Stepanov on 19.02.18.
 */
public class HighBidReducer extends Reducer<IntWritable, LongWritable, Text, LongWritable> {

  private static final Logger logger = Logger.getLogger( HighBidReducer.class );


  private final Text cityName = new Text();
  private final LongWritable amount = new LongWritable();

  private HashMap<Integer, String> cityDictionary = new HashMap<>();

  @Override protected void setup( Context context ) throws IOException {
    //context.getCacheFiles()
    readMap( Constants.CITY_DICTIONARY_FILE_NAME_CN );
    //Rewrite chinese names in case have english localization
    readMap( Constants.CITY_DICTIONARY_FILE_NAME_EN );
  }

  private void readMap( String localFileName ) throws IOException {
    if ( !new File( localFileName ).exists() ) {
      return;
    }
    BufferedReader cityFile = new BufferedReader( new FileReader( localFileName ) );
    String line;
    while ( ( line = cityFile.readLine() ) != null ) {
      int separator = line.indexOf( '\t' );
      if ( separator > 0 ) {
        cityDictionary.put( Integer.valueOf( line.substring( 0, separator ) ), line.substring( separator + 1 ) );
      } else {
        logger.error( "Failed to parse city value " + line );
      }
    }
    cityFile.close();
  }

  @Override protected void reduce( IntWritable key, Iterable<LongWritable> values, Context context )
    throws IOException, InterruptedException {
    long am = 0;
    for ( LongWritable value : values ) {
      am += value.get();
    }
    int cityId = key.get();
    String city = cityDictionary.get( cityId );

    if ( city == null ) {
      logger.error( "Not found mapping for city id " + cityId );
      cityName.set( String.valueOf( cityId ) );
    } else {
      cityName.set( city );
    }
    amount.set( am );
    context.write( cityName, amount );
  }
}
