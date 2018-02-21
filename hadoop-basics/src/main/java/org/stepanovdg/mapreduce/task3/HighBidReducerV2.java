package org.stepanovdg.mapreduce.task3;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;
import org.stepanovdg.mapreduce.task3.writable.IntTextPairWritable;

import java.io.IOException;
import java.util.HashMap;

import static org.stepanovdg.mapreduce.task3.util.DictionaryUtil.readMap;

/**
 * Created by Dmitriy Stepanov on 19.02.18.
 */
public class HighBidReducerV2 extends Reducer<IntTextPairWritable, LongWritable, Text, LongWritable> {

  private static final Logger logger = Logger.getLogger( HighBidReducerV2.class );
  private static final byte[] separatorBytes = new Text( "\t" ).getBytes();
  private static final int separatorBytesLength = separatorBytes.length;
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


  @Override protected void reduce( IntTextPairWritable key, Iterable<LongWritable> values, Context context )
    throws IOException, InterruptedException {
    long am = 0;
    for ( LongWritable value : values ) {
      am += value.get();
    }
    int cityId = key.getOne().get();
    String city = cityDictionary.get( cityId );


    if ( city == null ) {
      logger.error( "Not found mapping for city id " + cityId );
      cityName.set( String.valueOf( cityId ) );
    } else {
      cityName.set( city );
    }

    //iF we need grouping by os type
    byte[] bytes = key.getTwo().getBytes();

    cityName.append( separatorBytes, 0, separatorBytesLength );
    cityName.append( bytes, 0, key.getTwo().getLength() );


    amount.set( am );
    context.write( cityName, amount );
  }
}
