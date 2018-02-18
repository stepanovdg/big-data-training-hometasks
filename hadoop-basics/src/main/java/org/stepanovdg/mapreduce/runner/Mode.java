package org.stepanovdg.mapreduce.runner;

/**
 * Created by Dmitriy Stepanov on 17.02.18.
 */
public enum Mode {
  LONGEST_WORD_v1, LONGEST_WORD_v2, PARSE_LOGS_FORIP_v1, PARSE_LOGS_FORIP_v2;

  public static String generateModeHelp() {
    StringBuilder b = new StringBuilder();
    for ( Mode m : Mode.values() ) {
      b.append( "\t" ).append( "-" ).append( m.ordinal() ).append( ":" ).append( m ).append( "\n\r" );
    }
    return b.toString();
  }

}
