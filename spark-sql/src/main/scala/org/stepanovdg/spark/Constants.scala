package org.stepanovdg.spark

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, length, lit, when}
import org.apache.spark.sql.types._
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}

object Constants {

  val DELIMITER = ","

  val CSV_FORMAT = "com.databricks.spark.csv"
  val PRECISION = 3

  val motelID = "MotelID"
  val bidDate = "BidDate"
  val losa = "LoSa"
  val price = "Price"
  val errors = "errors"
  val BIDS_HEADER = Seq(motelID, bidDate, "HU", "UK", "NL", "US", "MX", "AU", "CA", "CN", "KR", "BE", "I", "JP", "IN", "HN", "GY", "DE")
  val MOTELS_HEADER = Seq(motelID, "MotelName", "Country", "URL", "Comment")
  val EXCHANGE_RATES_HEADER = Seq("ValidFrom", "CurrencyName", "CurrencyCode", "ExchangeRate")
  val third_col_with_error_mark = BIDS_HEADER(2)
  val ERROR_HEADER = Seq(bidDate, third_col_with_error_mark, errors)
  val ENRICHED_HEADER = Seq(motelID, MOTELS_HEADER(1), bidDate, losa, price)


  //Do not understand why it is not possible with nullable=false
  val EXCHANGE_RATES_SCHEMA = StructType(Seq(
    StructField(EXCHANGE_RATES_HEADER(0), StringType, true),
    StructField(EXCHANGE_RATES_HEADER(1), StringType, true),
    StructField(EXCHANGE_RATES_HEADER(2), StringType, true),
    StructField(EXCHANGE_RATES_HEADER(3), DoubleType, true)
  ))
  val EXCHANGE_RATES_SCHEMA_SHORT = StructType(Seq(
    StructField(EXCHANGE_RATES_HEADER(0), StringType, true),
    StructField(EXCHANGE_RATES_HEADER(3), DoubleType, true)
  ))

  val MOTELS_SCHEMA = StructType(MOTELS_HEADER
    .map(field => StructField(field, StringType, false)))

  val MOTELS_OUTPUT_SCHEMA = StructType(Seq(
    StructField(MOTELS_HEADER(0), StringType, true),
    StructField(MOTELS_HEADER(1), StringType, true)
  ))

  val ENRICHED_BIDS_SCHEMA = StructType(ENRICHED_HEADER
    .map(field => {
      if (field.equals(ENRICHED_HEADER.last)) {
        StructField(field, DoubleType, true)
      } else {
        StructField(field, StringType, true)
      }
    }))

  val BIDS_SCHEMA = StructType(BIDS_HEADER
    .map(field => StructField(field, StringType, true)))

  val ERROR_SCHEMA = StructType(Seq(
    StructField(ERROR_HEADER(0), StringType, true),
    StructField(ERROR_HEADER(1), StringType, true),
    StructField(ERROR_HEADER(2), LongType, false)
  ))

  val BID_OUTPUT_SCHEMA = StructType(Seq(
    StructField(BIDS_HEADER(0), StringType, true),
    StructField(BIDS_HEADER(1), StringType, true),
    StructField(losa, StringType, true),
    StructField(price, DoubleType, true)
  ))

  val TARGET_LOSAS = Seq("US", "CA", "MX")
  val INPUT_DATE_FORMAT: DateTimeFormatter = DateTimeFormat.forPattern("HH-dd-MM-yyyy")
  val OUTPUT_DATE_FORMAT: DateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm")

  def nullifyEmptyStrings(rawBids: DataFrame): Unit = {
    var in = rawBids
    for (e <- rawBids.columns) {
      in = in.withColumn(e, when(length(col(e)) === 0, lit(null: String)).otherwise(col(e)))
    }
  }
}
