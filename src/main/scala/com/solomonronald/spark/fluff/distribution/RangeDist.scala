package com.solomonronald.spark.fluff.distribution

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.round

class RangeDist(min: Double = 0.00, max: Double = 1.00, precision: Int = 16) extends FluffyDistribution with Serializable {

  override def getColumn(c: Column): Column = {
    round((c * (max - min)) + min, precision)
  }

  override def toString: String = {
    s"range(min: $min, max: $max, precision: $precision)"
  }
}

object RangeDist {
  val NAME_ID: String = "rang"

  def parse(expr: String): RangeDist = {
    val input: Array[String] = expr.substring(6, expr.length - 1)
      .split(",")
      .map(s => s.trim)

    if (input.length > 2) {
      new RangeDist(input(0).toDouble, input(1).toDouble, input(2).toInt)
    } else {
      new RangeDist(input(0).toDouble, input(1).toDouble)
    }
  }
}