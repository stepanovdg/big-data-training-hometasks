package org.stepanovdg.spark.domain


object EnrichedItem {
  def apply(bidItem: BidItem, name: String): EnrichedItem = new EnrichedItem(bidItem, name)
}

case class EnrichedItem(motelId: String, motelName: String, bidDate: String, loSa: String, price: Double) {

  def this(bidItem: BidItem, name: String) {
    this(bidItem.motelId, name, bidItem.bidDate, bidItem.loSa, bidItem.price)
  }

  override def toString: String = s"$motelId,$motelName,$bidDate,$loSa,$price"
}