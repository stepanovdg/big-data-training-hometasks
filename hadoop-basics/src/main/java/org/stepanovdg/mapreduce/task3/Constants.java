package org.stepanovdg.mapreduce.task3;

/**
 * Created by Dmitriy Stepanov on 19.02.18.
 */
public class Constants {

  public static final String IMP_REGEX =
    "\\A([^\\t]+)(\\t)([^\\t]+)(\\t)([^\\t]+)(\\t)([^\\t]+)(\\t)([^\\t]+)(\\t)([^\\t]+)(\\t)([^\\t]+)(\\t)([^\\t]+)"
      + "(\\t)([^\\t]+)(\\t)([^\\t]+)(\\t)([^\\t]+)(\\t)([^\\t]+)(\\t)([^\\t]+)(\\t)([^\\t]+)(\\t)([^\\t]+)(\\t)"
      + "([^\\t]+)(\\t)([^\\t]+)(\\t)([^\\t]+)(\\t)([^\\t]+)(\\t)([^\\t]+)(\\t)([^\\t]+)(\\t)([^\\t]+)(\\t)([^\\t]+)"
      + "(\\t)([^\\t]+)\\z";

  public static final String IMP_REGEX_NAMES = "\\A([^\\t]+)(\\t)([^\\t]+)(\\t)([^\\t]+)(\\t)([^\\t]+)(\\t)([^\\t]+)"
    + "(\\t)([^\\t]+)(\\t)([^\\t]+)(\\t)(?<cityId>[^\\t]+)(\\t)([^\\t]+)(\\t)([^\\t]+)(\\t)([^\\t]+)(\\t)([^\\t]+)"
    + "(\\t)([^\\t]+)(\\t)([^\\t]+)(\\t)([^\\t]+)(\\t)([^\\t]+)(\\t)([^\\t]+)(\\t)([^\\t]+)(\\t)([^\\t]+)(\\t)"
    + "(?<bid>[^\\t]+)(\\t)([^\\t]+)(\\t)([^\\t]+)(\\t)([^\\t]+)(\\t)([^\\t]+)\\z";

  public static final int CITY_ID = 8;
  public static final int BID = 20;
  public static final String BORDER_BID_PRICE_PROPERTY_NAME = "border.bid.price";
  public static final String CITY_DICTIONARY_FILE_NAME_EN = "city.en.txt";
  public static final String CITY_DICTIONARY_FILE_NAME_CN = "city.cn.txt";
}
