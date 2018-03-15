package org.stepanovdg.spark.domain

object BidItem {
  def apply(motelId: String, bidDate: String, loSa: String, priceNotRounded: Double, roundPrec: Int): BidItem
  = new BidItem(motelId, bidDate, loSa, round(priceNotRounded, roundPrec))

  implicit def round(value: Double, places: Int): Double = {
    if (places < 0) throw new IllegalArgumentException
    val factor = Math.pow(10, places).toLong
    val value2 = value * factor
    val tmp = value2.round
    tmp.toDouble / factor
  }
}

case class BidItem(motelId: String, bidDate: String, loSa: String, price: Double) {

  def this(motelId: String, bidDate: String, loSa: String, priceNotRounded: Double, roundPrec: Int) {
    this(motelId, bidDate, loSa, BidItem.round(priceNotRounded, roundPrec))
  }


  override def toString: String = {
    //val roundedPrice = round(price, 3)
    s"$motelId,$bidDate,$loSa,$price"
  }
}
