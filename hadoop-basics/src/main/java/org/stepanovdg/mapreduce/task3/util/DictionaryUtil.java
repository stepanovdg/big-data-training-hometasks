package org.stepanovdg.mapreduce.task3.util;

import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;

/**
 * Created by Dmitriy Stepanov on 20.02.18.
 */
public class DictionaryUtil {
  public static void readMap( Map<Integer, String> cityDictionary, String localFileName, Logger logger )
    throws IOException {
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
}
