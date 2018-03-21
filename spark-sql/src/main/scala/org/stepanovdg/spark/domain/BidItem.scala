package org.stepanovdg.spark.domain

import org.apache.spark.sql.Row

object BidItem {
  def apply(motelId: String, bidDate: String, loSa: String, priceNotRounded: Double, roundPrec: Int): BidItem
  = new BidItem(motelId, bidDate, loSa, round(priceNotRounded, roundPrec).toString)

  def apply(motelId: String, bidDate: String, loSa: String, price: Double): BidItem
  = new BidItem(motelId, bidDate, loSa, price.toString)

  implicit def round(value: Double, places: Int): Double = {
    if (places < 0) throw new IllegalArgumentException
    // Correct mode in expected results HALF_UP
    BigDecimal(value).setScale(places, BigDecimal.RoundingMode.HALF_UP).toDouble
  }
}

case class BidItem(motelId: String, bidDate: String, loSa: String, price: String) {

  def this(motelId: String, bidDate: String, loSa: String, priceNotRounded: Double, roundPrec: Int) {
    this(motelId, bidDate, loSa, BidItem.round(priceNotRounded, roundPrec).toString)
  }


  override def toString: String = {
    //val roundedPrice = round(price, 3)
    s"$motelId,$bidDate,$loSa,$price"
  }

  def toRow: Row = {
    Row(motelId, bidDate, loSa, price)
  }
}
