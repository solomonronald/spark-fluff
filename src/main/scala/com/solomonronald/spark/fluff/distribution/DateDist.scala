package com.solomonronald.spark.fluff.distribution
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{from_unixtime, lit, to_date, to_timestamp, unix_timestamp}

class DateDist(startDateStr: String, endDateStr: String, format: String) extends FluffyDistribution with Serializable {

  override def getColumn(c: Column): Column = {
    val min = unix_timestamp(lit(startDateStr), format)
    val max = unix_timestamp(lit(endDateStr), format)
    val timestamp: Column = (c * (max - min)) + min
    from_unixtime(timestamp, format)
  }

  override def toString: String = {
    s"date(start: $startDateStr, end: $endDateStr, format: $format)"
  }
}

object DateDist {
  val NAME_ID: String = "date"

  def parse(expr: String): DateDist = {
    val input: Array[String] = expr.substring(5, expr.length - 1)
      .split(",")
      .map(s => s.trim)

    new DateDist(input(0), input(1), input(2))
  }
}
