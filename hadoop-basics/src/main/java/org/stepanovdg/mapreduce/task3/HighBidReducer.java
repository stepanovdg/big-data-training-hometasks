package org.stepanovdg.mapreduce.task3;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashMap;

import static org.stepanovdg.mapreduce.task3.util.DictionaryUtil.readMap;

/**
 * Created by Dmitriy Stepanov on 19.02.18.
 */
public class HighBidReducer extends Reducer<IntWritable, LongWritable, Text, LongWritable> {

  private static final Logger logger = Logger.getLogger( HighBidReducer.class );


  private final Text cityName = new Text();
  private final LongWritable amount = new LongWritable();

  private HashMap<Integer, String> cityDictionary = new HashMap<>();

  @Override protected void setup( Context context ) throws IOException {
    readMap( cityDictionary, Constants.CITY_DICTIONARY_FILE_NAME_CN, logger );
    //Rewrite chinese names in case have english localization
    readMap( cityDictionary, getFileNameEn( context ), logger );
  }

  @VisibleForTesting
  protected String getFileNameEn( Context context ) throws IOException {
    return Constants.CITY_DICTIONARY_FILE_NAME_EN;
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
