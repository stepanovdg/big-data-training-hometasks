package org.stepanovdg.spark.domain

import org.stepanovdg.spark.Constants

case class BidErrorGroupBy(bidError: BidError, iterable: Iterable[BidError]) {

  override def toString: String = s"$bidError${Constants.DELIMITER}${iterable.size}"
}
