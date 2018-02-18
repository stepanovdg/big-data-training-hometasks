package org.stepanovdg.mapreduce.task2;

/**
 * Created by Dmitriy Stepanov on 18.02.18.
 */
public class Constants {

  public static final String AGG_LOG_PATTERN_NAME = "agg.log.pattern";
  public static final String OUT_SEPARATOR = ",";
  static final String REGEX = "ip(?<ip>\\d*) - - \\[.*\\] \".*\" \\d\\d\\d (?<bytes>\\d+) \".*\" "
    + "(?<userAgent>\".*\")+";
  static final String REGEX_304 = "ip(?<ip>\\d*) - - \\[.*\\] \".*\" \\d\\d\\d (?<bytes>-) \".*\" "
    + "(?<userAgent>\".*\")+";
  static final char TEMP_SEPARATOR = ':';

}
