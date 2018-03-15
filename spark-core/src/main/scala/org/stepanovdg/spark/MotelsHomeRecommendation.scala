package org.stepanovdg.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.stepanovdg.spark.domain._

import scala.collection.JavaConversions._

object MotelsHomeRecommendation {

  val ERRONEOUS_DIR: String = "erroneous"
  val AGGREGATED_DIR: String = "aggregated"

  def main(args: Array[String]): Unit = {
    require(args.length == 4, "Provide parameters in this order: bidsPath, motelsPath, exchangeRatesPath, outputBasePath")

    val bidsPath = args(0)
    val motelsPath = args(1)
    val exchangeRatesPath = args(2)
    val outputBasePath = args(3)

    val conf = new SparkConf()

    val sc = new SparkContext(conf.setAppName("motels-home-recommendation"))

    processData(sc, bidsPath, motelsPath, exchangeRatesPath, outputBasePath)

    sc.stop()
  }

  def processData(sc: SparkContext, bidsPath: String, motelsPath: String, exchangeRatesPath: String, outputBasePath: String) = {

    /**
      * Task 1:
      * Read the bid data from the provided file.
      */
    val rawBids: RDD[List[String]] = getRawBids(sc, bidsPath)

    /**
      * Task 1:
      * Collect the errors and save the result.
      * Hint: Use the BideError case class
      */
    val erroneousRecords: RDD[String] = getErroneousRecords(rawBids)
    erroneousRecords.saveAsTextFile(s"$outputBasePath/$ERRONEOUS_DIR")

    /**
      * Task 2:
      * Read the exchange rate information.
      * Hint: You will need a mapping between a date/time and rate
      */
    val exchangeRates: Map[String, Double] = getExchangeRates(sc, exchangeRatesPath)

    /**
      * Task 3:
      * Transform the rawBids and use the BidItem case class.
      * - Convert USD to EUR. The result should be rounded to 3 decimal precision.
      * - Convert dates to proper format - use formats in Constants util class
      * - Get rid of records where there is no price for a Losa or the price is not a proper decimal number
      */
    val bids: RDD[BidItem] = getBids(rawBids, exchangeRates)

    /**
      * Task 4:
      * Load motels data.
      * Hint: You will need the motels name for enrichment and you will use the id for join
      */
    val motels: RDD[(String, String)] = getMotels(sc, motelsPath)

    /**
      * Task5:
      * Join the bids with motel names and utilize EnrichedItem case class.
      * Hint: When determining the maximum if the same price appears twice then keep the first entity you found
      * with the given price.
      */
    //val enriched: RDD[EnrichedItem] = getEnriched(bids, motels)
    val enriched: RDD[EnrichedItem] = getEnrichedMapJoin(sc, bids, motels)
    enriched.saveAsTextFile(s"$outputBasePath/$AGGREGATED_DIR")
  }

  def getRawBids(sc: SparkContext, bidsPath: String): RDD[List[String]] = {
    val rawb = sc.textFile(bidsPath).map(s => {
      s.split(Constants.DELIMITER).toList
    })
    //For cluster persisit is good - for local - worse than without it
    rawb.persist(StorageLevel.MEMORY_AND_DISK)
  }

  def getErroneousRecords(rawBids: RDD[List[String]]): RDD[String] = {
    val value = rawBids.filter(f => f.get(2).matches("ERROR_(.*)"))
    //  val value = rawBids.filter(f => f.lengthCompare(3) != 1)
    val value2 = value.map(l => BidError(l.get(1), l.get(2)))

    /// Do not understand how to tuple to  => [BidErrorGroupBy]
    value2.groupBy(f => f).map(f => f._1.toString() + Constants.DELIMITER + f._2.size)
  }

  //It is bad example better to use big decimal for exact manipulations with money
  def getExchangeRates(sc: SparkContext, exchangeRatesPath: String): Map[String, Double] = {
    val rat = sc.textFile(exchangeRatesPath).map(s => {
      val a = s.split(Constants.DELIMITER)
      (a(0), a(3).toDouble)
    })
    rat.collectAsMap().toMap
    //DO not understand why it is not possible to use implicit in ExchangeRate
  }

  def getBids(rawBids: RDD[List[String]], exchangeRates: Map[String, Double]): RDD[BidItem] = {

    implicit def dateFun(list: List[String], func: Int => String): List[String] = {
      func(Constants.indexMap(Constants.bidDate)) :: list
    }

    implicit def idFun(list: List[String], func: Int => String): List[String] = {
      func(Constants.indexMap(Constants.motelID)) :: list
    }

    implicit def priceFun(list: List[String], func: Int => String, losa: String): List[String] = {
      func(Constants.indexMap(losa)) :: list
    }

    implicit def constr(func: Int => String): List[Seq[String]] = {
      //understand this is not best approach wasn`t able to find how to create better function to create such List of List
      var list_id: List[String] = List()
      var list_date: List[String] = List()
      var list_price: List[String] = List()
      Constants.TARGET_LOSAS.foreach(losa => {
        list_id = idFun(list_id, func)
        list_date = dateFun(list_date, func)
        list_price = priceFun(list_price, func, losa)
      })
      List(list_id, list_date, Constants.TARGET_LOSAS, list_price.reverse)
    }

    /*    val tran2 = rawBids.filter(f => !f.get(2).matches("ERROR_(.*)")).map(f =>
          List(
            List(f.get(0), f.get(0), f.get(0)),
            List(f.get(1), f.get(1), f.get(1)),
            Constants.TARGET_LOSAS,
            List(f.get(5), f.get(6), f.get(8))))
          .flatMap(f => f.transpose)*/

    val tran = rawBids.filter(f => !f.get(2).matches("ERROR_(.*)")).map(f => constr(f.get)).flatMap[List[String]](f => f.transpose)
    val checkIfPriceIsValid: List[String] => Boolean = f => {
      try {
        f.get(3).toDouble
        true
      } catch {
        case e1: NumberFormatException => false
      }
    }

    def transformDate(date: String): String = {
      Constants.INPUT_DATE_FORMAT.parseDateTime(date).toString(Constants.OUTPUT_DATE_FORMAT)
    }

    tran.filter(checkIfPriceIsValid).map(f => {
      //first shoud not round price as it would get more corerct result in findnig max - after round
      BidItem(f.get(0), transformDate(f.get(1)), f.get(2), f.get(3).toDouble * exchangeRates(f.get(1)))
    })
  }

  def getMotels(sc: SparkContext, motelsPath: String): RDD[(String, String)] = {
    sc.textFile(motelsPath).map(f => {
      val a = f.split(',')
      (a(0), a(1))
    })
  }

  def getEnrichedMapJoin(sc: SparkContext, bids: RDD[BidItem], motels: RDD[(String, String)]): RDD[EnrichedItem] = {
    val maxes = bids.map(b => (BidIdDateKey(b.motelId, b.bidDate), BidLosaPriceValue(b.loSa, b.price))).groupByKey().map(m => {
      val it = m._2
      //DO not able to compile if one line
      val max_for_date_motel: BidLosaPriceValue = it.max
      new BidItem(m._1.id, m._1.date, max_for_date_motel.losa, max_for_date_motel.price, Constants.PRECISION)
    })
    //IF motels number is enough to suit to memory - we could use broadcast for mapper join
    val motelsBrod = sc.broadcast(motels.collectAsMap())

    maxes.map[EnrichedItem](bidItem => {
      val maybeMotelName = motelsBrod.value.get(bidItem.motelId)
      if (maybeMotelName.isDefined) {
        new EnrichedItem(bidItem, maybeMotelName.get)
      } else {
        new EnrichedItem(bidItem, Constants.NOT_FOUND_MOTEL_NAME)
      }
    })
  }

  def getEnriched(bids: RDD[BidItem], motels: RDD[(String, String)]): RDD[EnrichedItem] = {
    val maxes = bids.map(b => (BidIdDateKey(b.motelId, b.bidDate), BidLosaPriceValue(b.loSa, b.price))).groupByKey().map(m => {
      val it = m._2
      //DO not able to compile if one line
      val max_for_date_motel: BidLosaPriceValue = it.max
      new BidItem(m._1.id, m._1.date, max_for_date_motel.losa, max_for_date_motel.price, Constants.PRECISION)
    })
    //IF motels number is enough to suit to memory - we could use broadcast for mapper join
    maxes.map(b => (b.motelId, b)).join(motels).map(joinResult => {
      new EnrichedItem(joinResult._2._1, joinResult._2._2)
    })
  }
}
