package org.stepanovdg.spark


import org.apache.spark.sql.expressions.{UserDefinedFunction, Window}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{Column, DataFrame, Row}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.stepanovdg.spark.Constants.{motelID, _}
import org.stepanovdg.spark.domain.BidItem

object MotelsHomeRecommendation {

  val ERRONEOUS_DIR: String = "erroneous"
  val AGGREGATED_DIR: String = "aggregated"

  def main(args: Array[String]): Unit = {
    require(args.length == 4, "Provide parameters in this order: bidsPath, motelsPath, exchangeRatesPath, outputBasePath")

    val bidsPath = args(0)
    val motelsPath = args(1)
    val exchangeRatesPath = args(2)
    val outputBasePath = args(3)

    val sc = new SparkContext(new SparkConf().setAppName("motels-home-recommendation"))
    val sqlContext = new HiveContext(sc)


    processData(sqlContext, bidsPath, motelsPath, exchangeRatesPath, outputBasePath)

    sc.stop()
  }

  def processData(sqlContext: HiveContext, bidsPath: String, motelsPath: String, exchangeRatesPath: String, outputBasePath: String) = {

    /**
      * Task 1:
      * Read the bid data from the provided file.
      */
    val rawBids: DataFrame = getRawBids(sqlContext, bidsPath)

    /**
      * Task 1:
      * Collect the errors and save the result.
      */
    val erroneousRecords: DataFrame = getErroneousRecords(rawBids)
    erroneousRecords.write
      .format(Constants.CSV_FORMAT)
      .save(s"$outputBasePath/$ERRONEOUS_DIR")

    /**
      * Task 2:
      * Read the exchange rate information.
      * Hint: You will need a mapping between a date/time and rate
      */
    val exchangeRates: DataFrame = getExchangeRates(sqlContext, exchangeRatesPath)

    /**
      * Task 3:
      * UserDefinedFunction to convert between date formats.
      * Hint: Check the formats defined in Constants class
      */
    val convertDate: UserDefinedFunction = getConvertDate

    /**
      * Task 3:
      * Transform the rawBids
      * - Convert USD to EUR. The result should be rounded to 3 decimal precision.
      * - Convert dates to proper format - use formats in Constants util class
      * - Get rid of records where there is no price for a Losa or the price is not a proper decimal number
      */
    val bids: DataFrame = getBids(rawBids, exchangeRates)

    /**
      * Task 4:
      * Load motels data.
      * Hint: You will need the motels name for enrichment and you will use the id for join
      */
    val motels: DataFrame = getMotels(sqlContext, motelsPath)

    /**
      * Task5:
      * Join the bids with motel names.
      */
    val enriched: DataFrame = getEnriched(bids, motels)
    enriched.write
      .format(Constants.CSV_FORMAT)
      .save(s"$outputBasePath/$AGGREGATED_DIR")
  }

  def getRawBids(sqlContext: HiveContext, bidsPath: String): DataFrame = {
    val rawb = sqlContext.read.schema(BIDS_SCHEMA).parquet(bidsPath)
    //run explain
    rawb.persist(StorageLevel.MEMORY_AND_DISK)
  }


  def getErroneousRecords(rawBids: DataFrame): DataFrame = {
    rawBids.where(col(third_col_with_error_mark) rlike "ERROR_(.*)")
      .select(bidDate, third_col_with_error_mark)
      .groupBy(bidDate, third_col_with_error_mark)
      .count()
      .withColumnRenamed("count", errors)
  }

  def getExchangeRates(sqlContext: HiveContext, exchangeRatesPath: String): DataFrame = {
    sqlContext.read.schema(EXCHANGE_RATES_SCHEMA).csv(exchangeRatesPath)
      .select(EXCHANGE_RATES_HEADER(0), EXCHANGE_RATES_HEADER(3))
    //DO not understand why after csv schema became nullable true
  }

  def getConvertDate: UserDefinedFunction = {
    //Wasn`t able to specify return schema

    UserDefinedFunction((date: String) => Constants.INPUT_DATE_FORMAT.parseDateTime(date).toString(Constants.OUTPUT_DATE_FORMAT),
      StringType, Option(Seq(StringType)))
  }

  def getBids(rawBids: DataFrame, exchangeRates: DataFrame): DataFrame = {
    val explodeCol = "explodeCol"
    val mapCol = "explodeCol2"
    val strings = motelID +: bidDate +: explodeCol +: TARGET_LOSAS
    val target_losas_map = TARGET_LOSAS.flatMap(f => {
      Array((0, f), (1, f))
    })

    def createMapUDF(): Column = {
      map(target_losas_map.map(f => {
        if (f._1 == 0) {
          lit(f._2)
        } else {
          col(f._2)
        }
      }): _*)
    }

    val createRowsUDF = udf((motelID: String, bidDate: String, losas_prices: Map[String, String]) => {
      (for (losa <- TARGET_LOSAS) yield
        BidItem(motelID, bidDate, losa, losas_prices(losa))
        ).toArray
    })
    val checkIfPriceIsValid: Row => Boolean = f => {
      try {
        f.getString(3).toDouble
        true
      } catch {
        case e1: NumberFormatException => false
        case e2: NullPointerException => false
      }
    }
    val toDoubleUDF = udf((s: String) => s.toDouble)
    rawBids.where(!(col(third_col_with_error_mark) rlike "ERROR_(.*)"))
      .withColumn(mapCol, createMapUDF())
      .withColumn(explodeCol, createRowsUDF(col(motelID), col(bidDate), col(mapCol)))
      .select(explode(col(explodeCol)).alias("b")).select(col("b")(motelID).alias(motelID),
      col("b")(bidDate).alias(bidDate), col("b")(losa).alias(losa), col("b")(price).alias(price))
      .filter(checkIfPriceIsValid)
      .join(exchangeRates, col(bidDate) <=> col(EXCHANGE_RATES_HEADER.head))
      .select(col(motelID), getConvertDate(col(bidDate)).alias(bidDate), col(losa), (toDoubleUDF(col(price)) * col(EXCHANGE_RATES_HEADER(3))).alias(price))
  }

  def getMotels(sqlContext: HiveContext, motelsPath: String): DataFrame = {
    sqlContext.read.schema(MOTELS_SCHEMA).parquet(motelsPath).select(col(MOTELS_HEADER(0)), col(MOTELS_HEADER(1)))
  }

  def getEnriched(bids: DataFrame, motels: DataFrame): DataFrame = {
    val roundUdf = udf((value: Double) => BidItem.round(value, PRECISION))
    bids.withColumn("rank", dense_rank().over(Window.partitionBy(col(motelID), col(bidDate)).orderBy(col(price).desc)))
      .where(col("rank") === 1).drop(col("rank"))
      .join(broadcast(motels), bids(motelID) <=> motels(motelID)).drop(motels(motelID))
      .withColumn("round_price", roundUdf(col(price)))
      .drop(col(price)).withColumnRenamed("round_price", price)
      .select(ENRICHED_HEADER.map(col): _*)
  }
}
