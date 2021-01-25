package com.solomonronald.spark.fluff.types
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{from_unixtime, lit, unix_timestamp}

class DateFluff(startDateStr: String, endDateStr: String, format: String) extends FluffType with Serializable {
  private val serialVersionUID = 3192225079626485872L
  override val needsRandomIid: Boolean = true

  override def getColumn(c: Column): Column = {
    val min = unix_timestamp(lit(startDateStr), format)
    val max = unix_timestamp(lit(endDateStr), format)
    val timestamp: Column = (c * (max - min)) + min
    from_unixtime(timestamp, format)
  }

  override def toString: String = s"dateFluff(start: $startDateStr, end: $endDateStr, format: $format)"

}

object DateFluff extends FluffObjectType{
  val NAME_ID: String = "date"

  def parse(expr: String): DateFluff = {
    val input: Array[String] = expr.substring(5, expr.length - 1)
      .split(",")
      .map(s => s.trim)

    new DateFluff(input(0), input(1), input(2))
  }
}
