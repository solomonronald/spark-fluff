package com.solomonronald.spark.fluff.types

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.round

/**
 * [[FluffType]] Function to show range of double value from min to max with custom precision.
 * Range returned will be [min, max)
 * @param min min value. Inclusive
 * @param max max value. Exclusive.
 * @param precision number of decimal precision to return for the column
 */
class RangeFluff(min: Double = 0.00, max: Double = 1.00, precision: Int = 16) extends FluffType with Serializable {
  private val serialVersionUID = 7226067891252319122L
  override val needsRandomIid: Boolean = true

  override def getColumn(c: Column): Column = {
    round((c * (max - min)) + min, precision)
  }

  override def toString: String = s"rangeFluff(min: $min, max: $max, precision: $precision)"

}

object RangeFluff extends FluffObjectType {
  val NAME_ID: String = "rang"

  /**
   * Parser for range function expression
   * @param expr range function expr
   * @return
   */
  def parse(expr: String): RangeFluff = {
    // Get range parameters from expr string "range(...)"
    val input: Array[String] = expr.substring(6, expr.length - 1)
      .split(",")
      .map(s => s.trim)

    // If range has only 2 parameters then set min and max value only, else set all values
    if (input.length > 2) {
      new RangeFluff(input(0).toDouble, input(1).toDouble, input(2).toInt)
    } else {
      new RangeFluff(input(0).toDouble, input(1).toDouble)
    }
  }
}