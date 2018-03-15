package org.stepanovdg.spark.domain

case class BidError(date: String, errorMessage: String) {

  override def toString: String = s"$date,$errorMessage"
}
