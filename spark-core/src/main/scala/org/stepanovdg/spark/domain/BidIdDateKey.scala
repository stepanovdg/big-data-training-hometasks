package org.stepanovdg.spark.domain

case class BidIdDateKey(id: String, date: String) {

  override def toString: String = s"$id,$date"
}
