package org.stepanovdg.spark

import java.io.File
import java.nio.file.Files

import com.holdenkarau.spark.testing.{DataFrameSuiteBase, RDDComparisons}
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.junit._
import org.scalatest.{BeforeAndAfter, FunSuite}
import org.stepanovdg.spark.Constants._
import org.stepanovdg.spark.MotelsHomeRecommendation.{AGGREGATED_DIR, ERRONEOUS_DIR}
import org.stepanovdg.spark.util.RddComparator

/**
  * Created by Csaba_Bejan on 8/17/2016.
  */
class MotelsHomeRecommendationTestingBaseTest extends FunSuite with BeforeAndAfter with DataFrameSuiteBase with RDDComparisons {

  val INPUT_BIDS_SAMPLE = "src/test/resources/bids_sample.txt"
  val INPUT_BIDS_SAMPLE_PARQUET = "src/test/resources/bids_sample.gz.parquet"
  val INPUT_MOTELS_SAMPLE = "src/test/resources/motels_sample.txt"
  val INPUT_MOTELS_SAMPLE_PARQUET = "src/test/resources/motels_sample.gz.parquet"
  val INPUT_EXCHANGE_RATES_SAMPLE = "src/test/resources/exchange_rate_sample.txt"

  val INPUT_BIDS_INTEGRATION = "src/test/resources/integration/input/bids.gz.parquet"
  val INPUT_EXCHANGE_RATES_INTEGRATION = "src/test/resources/integration/input/exchange_rate.txt"
  val INPUT_MOTELS_INTEGRATION = "src/test/resources/integration/input/motels.gz.parquet"

  val EXPECTED_AGGREGATED_INTEGRATION = "src/test/resources/integration/expected_output/aggregated"
  val EXPECTED_ERRORS_INTEGRATION = "src/test/resources/integration/expected_output/expected_sql"
  var hc: HiveContext = null
  private var outputFolder: File = null

  override def conf = new SparkConf().setMaster("local[2]").setAppName("motels-home-recommendation test")

  before {
    outputFolder = Files.createTempDirectory("output").toFile
    hc = new HiveContext(sc)
  }

  after {
    outputFolder.delete
    hc = hc.newSession()
  }

  // context stopped in super
  override def afterAll() {
    super.afterAll()
  }

  test("should read raw bids") {
    val rddExpected = sc.parallelize(
      Seq(
        Row("0000002", "11-05-08-2016", "0.92", "1.68", "0.81", "0.68", "1.59", null, "1.63", "1.77", "2.06", "0.66", "1.53", null, "0.32", "0.88", "0.83", "1.01"),
        Row("0000001", "06-05-02-2016", "ERROR_NO_BIDS_FOR_HOTEL", null, null, null, null, null, null, null, null, null, null, null, null, null, null, null)
      )
    )

    val expected = hc.createDataFrame(rddExpected, Constants.BIDS_SCHEMA)
    //val expected = rddExpected.toDF()

    //sqlContext.read.schema(Constants.BIDS_SCHEMA).csv(INPUT_BIDS_SAMPLE).write.parquet(INPUT_BIDS_SAMPLE_PARQUET)

    val rawBids = MotelsHomeRecommendation.getRawBids(hc, INPUT_BIDS_SAMPLE_PARQUET)

    assertDataFrameEquals(expected, rawBids)
  }

  test("should collect erroneous records") {
    val bids = sc.parallelize(
      Seq(
        Row("1", "06-05-02-2016", "ERROR_1", null, null, null, null, null, null, null, null, null, null, null, null, null, null, null),
        Row("2", "15-04-08-2016", "0.89", "0.91", null, null, null, null, null, null, null, null, null, null, null, null, null, null),
        Row("3", "07-05-02-2016", "ERROR_2", null, null, null, null, null, null, null, null, null, null, null, null, null, null, null),
        Row("4", "06-05-02-2016", "ERROR_1", null, null, null, null, null, null, null, null, null, null, null, null, null, null, null),
        Row("5", "06-05-02-2016", "ERROR_2", null, null, null, null, null, null, null, null, null, null, null, null, null, null, null)
      )
    )

    val rawBids = hc.createDataFrame(bids, Constants.BIDS_SCHEMA)

    val expectedRDD = sc.parallelize(
      Seq(
        Row("06-05-02-2016", "ERROR_1", 2l),
        Row("06-05-02-2016", "ERROR_2", 1l),
        Row("07-05-02-2016", "ERROR_2", 1l)
      )
    )
    val expected = hc.createDataFrame(expectedRDD, Constants.ERROR_SCHEMA).orderBy(bidDate, third_col_with_error_mark, errors)


    val erroneousRecords = MotelsHomeRecommendation.getErroneousRecords(rawBids).orderBy(bidDate, third_col_with_error_mark, errors)

    assertDataFrameEquals(expected, erroneousRecords)
  }

  test("getExchangeRates") {

    val expectedRDD = sc.parallelize(
      Seq(
        Row("11-06-05-2016", 0.803),
        Row("11-05-08-2016", 0.873)
      )
    )
    val expected = hc.createDataFrame(expectedRDD, EXCHANGE_RATES_SCHEMA_SHORT)

    val rates = MotelsHomeRecommendation.getExchangeRates(hc, INPUT_EXCHANGE_RATES_SAMPLE)

    assertDataFrameEquals(expected, rates)
  }

  test("udf") {

    val expectedRDD = sc.parallelize(
      Seq(
        Row("2016-05-06 11:00"),
        Row("2016-08-05 11:00")
      )
    )
    val date = "date"
    val expected = hc.createDataFrame(expectedRDD, StructType(Seq(StructField(date, StringType, true))))

    val inputRDD = sc.parallelize(
      Seq(
        Row("11-06-05-2016"),
        Row("11-05-08-2016")
      )
    )
    val input = hc.createDataFrame(inputRDD, StructType(Seq(StructField(date, StringType, true))))
    System.out.println("INPUT")
    input.show
    input.printSchema

    System.out.println("EXPECTED")
    expected.show
    expected.printSchema

    val out = input.select(MotelsHomeRecommendation.getConvertDate.apply(col(date)) as date)

    System.out.println("ACTUAL")
    out.show
    out.printSchema
    assertDataFrameEquals(expected, out)
  }

  test("should read bids") {

    val rawBidsRdd = sc.parallelize(
      Seq(
        Row("1", "06-05-02-2016", "ERROR_1", null, null, null, null, null, null, null, null, null, null, null, null, null, null, null),
        Row("2", "11-06-05-2016", "0.89", "0.91", "0.89", "0.91", "0.89", "0.91", "0.89", "0.91", "0.89", "0.91", "0.89", "0.91", "0.89", "0.91", "0.89", "0.91"),
        Row("3", "07-05-02-2016", "ERROR_2", null, null, null, null, null, null, null, null, null, null, null, null, null, null, null),
        Row("2", "11-05-08-2016", "0.98", "0.94", "0.98", "0.94", "0.98", "0.94", "0.98", "0.94", "0.98", "0.94", "0.98", "0.94", "0.89", "0.91", "0.89", "0.91")

      )
    )
    val rawBids = hc.createDataFrame(rawBidsRdd, Constants.BIDS_SCHEMA)


    val ratesRDD = sc.parallelize(
      Seq(
        Row("11-06-05-2016", 0.803),
        Row("11-05-08-2016", 0.873)
      )
    )
    val rates = hc.createDataFrame(ratesRDD, EXCHANGE_RATES_SCHEMA_SHORT)


    val expectedRdd = sc.parallelize(
      Seq(
        Row("2", "2016-05-06 11:00", "US", 0.7307300000000001),
        Row("2", "2016-05-06 11:00", "CA", 0.71467),
        Row("2", "2016-05-06 11:00", "MX", 0.71467),
        Row("2", "2016-08-05 11:00", "US", 0.8206199999999999),
        Row("2", "2016-08-05 11:00", "CA", 0.85554),
        Row("2", "2016-08-05 11:00", "MX", 0.85554)
      )
    )
    val expected = hc.createDataFrame(expectedRdd, Constants.BID_OUTPUT_SCHEMA)


    val bids = MotelsHomeRecommendation.getBids(rawBids, rates)
    assertDataFrameEquals(expected, bids)
  }

  test("should read motels") {

    //sqlContext.read.schema(Constants.MOTELS_SCHEMA).csv(INPUT_MOTELS_SAMPLE).write.parquet(INPUT_MOTELS_SAMPLE_PARQUET)

    val expectedRdd = sc.parallelize(
      Seq(
        Row("0000001", "Olinda Windsor Inn"),
        Row("0000002", "Merlin Por Motel")
      )
    )
    val expected = hc.createDataFrame(expectedRdd, Constants.MOTELS_OUTPUT_SCHEMA)


    val motels = MotelsHomeRecommendation.getMotels(hc, INPUT_MOTELS_SAMPLE_PARQUET)

    assertDataFrameEquals(expected, motels)
  }


  test("enriched") {

    val bidsRdd = sc.parallelize(
      Seq(
        Row("1", "2016-02-05 06:00", "US", 1d),
        Row("1", "2016-02-05 06:00", "MX", 2d),
        Row("1", "2016-02-05 06:00", "CA", 2d),
        Row("0000001", "2016-06-02 11:00", "MX", 1.50),
        Row("0000001", "2016-06-02 11:00", "US", 1.40),
        Row("0000001", "2016-06-02 11:00", "CA", 1.15),
        Row("0000005", "2016-06-02 12:00", "MX", 1.10),
        Row("0000005", "2016-06-02 12:00", "US", 1.20),
        Row("0000005", "2016-06-02 12:00", "CA", 1.30)
      )
    )

    val bids = hc.createDataFrame(bidsRdd, Constants.BID_OUTPUT_SCHEMA)

    val motelsRdd = sc.parallelize(
      Seq(
        Row("1", "myhotel"),
        Row("0000001", "Fantastic Hostel"),
        Row("0000005", "Majestic Ibiza Por Hostel")
      )
    )
    val motels = hc.createDataFrame(motelsRdd, Constants.MOTELS_OUTPUT_SCHEMA)

    val expectedRdd = sc.parallelize(
      Seq(
        Row("1", "myhotel", "2016-02-05 06:00", "CA", 2d),
        Row("1", "myhotel", "2016-02-05 06:00", "MX", 2d),
        Row("0000001", "Fantastic Hostel", "2016-06-02 11:00", "MX", 1.50),
        Row("0000005", "Majestic Ibiza Por Hostel", "2016-06-02 12:00", "CA", 1.30)
      )
    )
    val expected = hc.createDataFrame(expectedRdd, Constants.ENRICHED_BIDS_SCHEMA)


    val rawBids = MotelsHomeRecommendation.getEnriched(bids, motels)
    // Tried to value or def but was`n able to
    //ENRICHED_HEADER.map(col):_*
    assertDataFrameEquals(expected.sort(ENRICHED_HEADER.map(col): _*), rawBids.sort(ENRICHED_HEADER.map(col): _*))
  }

  test("should filter errors and create correct aggregates") {

    runIntegrationTest()

    //If the test fails and you are interested in what are the differences in the RDDs uncomment the corresponding line
    printRddDifferences(EXPECTED_ERRORS_INTEGRATION, getOutputPath(ERRONEOUS_DIR))
    printRddDifferences(EXPECTED_AGGREGATED_INTEGRATION, getOutputPath(AGGREGATED_DIR))

    assertRddTextFiles(EXPECTED_ERRORS_INTEGRATION, getOutputPath(ERRONEOUS_DIR))
    assertAggregatedFiles(EXPECTED_AGGREGATED_INTEGRATION, getOutputPath(AGGREGATED_DIR))
  }

  private def runIntegrationTest() = {
    MotelsHomeRecommendation.processData(hc, INPUT_BIDS_INTEGRATION, INPUT_MOTELS_INTEGRATION, INPUT_EXCHANGE_RATES_INTEGRATION, outputFolder.getAbsolutePath)
  }

  private def assertRddTextFiles(expectedPath: String, actualPath: String) = {
    val expected = sc.textFile(expectedPath)
    val actual = sc.textFile(actualPath)
    assertRDDEquals(expected, actual)
  }

  private def assertAggregatedFiles(expectedPath: String, actualPath: String) = {
    val expected = parseLastDouble(sc.textFile(expectedPath)).collect.toMap
    val actual = parseLastDouble(sc.textFile(actualPath)).collect.toMap
    if (expected.size != actual.size) {
      Assert.fail(s"Aggregated have wrong number of records (${actual.size} instead of ${expected.size})")
    }
    expected.foreach(x => {
      val key = x._1
      val expectedValue = x._2
      if (!actual.contains(key)) {
        Assert.fail(s"Aggregated does not contain: $key,$expectedValue")
      }
      val actualValue = actual(key)
      if (Math.abs(expectedValue - actualValue) > 0.0011) {
        Assert.fail(s"Aggregated have different value for: $key ($actualValue instead of $expectedValue)")
      }
    })
  }

  private def parseLastDouble(rdd: RDD[String]) = {
    rdd.map(s => {
      val commaIndex = s.lastIndexOf(",")
      (s.substring(0, commaIndex), s.substring(commaIndex + 1).toDouble)
    })
  }

  private def printRddDifferences(expectedPath: String, actualPath: String) = {
    val expected = sc.textFile(expectedPath)
    val actual = sc.textFile(actualPath)
    RddComparator.printDiff(expected, actual)
  }

  private def getOutputPath(dir: String): String = {
    new Path(outputFolder.getAbsolutePath, dir).toString
  }
}
