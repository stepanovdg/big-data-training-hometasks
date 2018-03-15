package org.stepanovdg.spark.domain

import org.stepanovdg.spark.Constants

case class BidLosaPriceValue(losa: String, price: Double) extends Ordered[BidLosaPriceValue] {

  override def toString: String = s"$losa,$price"

  override def compare(that: BidLosaPriceValue): Int = {
    val pr = this.price.compare(that.price)
    if (pr == 0) {
      -losaRank(this.losa).compare(losaRank(that.losa))
    } else {
      pr
    }
  }

  def losaRank(losa: String): Int = {
    Constants.TARGET_LOSAS.indexOf(losa)
    /*losa match {
      case "US" => 1
      case "MX" => 3
      case "CA" => 2
      case _ => 0
    }*/
  }


}
