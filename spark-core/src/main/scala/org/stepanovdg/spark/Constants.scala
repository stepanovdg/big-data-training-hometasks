package org.stepanovdg.spark

import org.joda.time.format.DateTimeFormat

object Constants {

  val DELIMITER = ","
  val PRECISION = 3
  val NOT_FOUND_MOTEL_NAME = "Not Found"

  val motelID = "MotelID"
  val bidDate = "BidDate"
  val BIDS_HEADER = Seq(motelID, bidDate, "HU", "UK", "NL", "US", "MX", "AU", "CA", "CN", "KR", "BE", "I", "JP", "IN", "HN", "GY", "DE")
  val MOTELS_HEADER = Seq(motelID, "MotelName", "Country", "URL", "Comment")
  val EXCHANGE_RATES_HEADER = Seq("ValidFrom", "CurrencyName", "CurrencyCode", "ExchangeRate")

  val TARGET_LOSAS = Seq("US", "CA", "MX")

  val INPUT_DATE_FORMAT = DateTimeFormat.forPattern("HH-dd-MM-yyyy")
  val OUTPUT_DATE_FORMAT = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm")

  val indexMap = BIDS_HEADER.map(header => header -> BIDS_HEADER.indexOf(header)).toMap
}
