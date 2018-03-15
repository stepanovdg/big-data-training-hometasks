package org.stepanovdg.spark.domain

import org.apache.spark.rdd.RDD

case class ExchangeRate(date: String, currencyName: String, currencyType: String, rate: Double) {

  implicit def string2ExchangeRate(s: String): ExchangeRate = {
    val a = s.split(",")
    ExchangeRate(a(0), a(1), a(2), a(3).toDouble)
  }

  implicit def stringRDD2ExchangeRateRDD(s: RDD[String]): RDD[ExchangeRate] = {
    s.map(string2ExchangeRate)
  }

  override def toString: String = s"$date,$currencyName,$currencyType,$rate"
}
