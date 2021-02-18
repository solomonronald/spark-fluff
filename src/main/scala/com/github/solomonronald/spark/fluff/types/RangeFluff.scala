package com.github.solomonronald.spark.fluff.types

import com.github.solomonronald.spark.fluff.common.Constants.DEFAULT_NULL_PERCENTAGE
import com.github.solomonronald.spark.fluff.common.FunctionParser
import com.github.solomonronald.spark.fluff.common.UtilFunctions.withNull
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.round

/**
 * [[FluffType]] Function to show range of double value from min to max with custom precision.
 * Range returned will be [min, max)
 * @param min min value. Inclusive
 * @param max max value. Exclusive.
 * @param precision number of decimal precision to return for the column
 * @param nullPercent null probability percentage
 */
class RangeFluff(min: Double = 0.00,
                 max: Double = 1.00,
                 precision: Int = 16,
                 nullPercent: Int = DEFAULT_NULL_PERCENTAGE
                ) extends FluffType with Serializable {
  private val serialVersionUID = 7226067891252319122L

  /**
   * This fluff requires a random iid
   */
  override val needsRandomIid: Boolean = true

  /**
   * Spark column expression to generate custom random column value.
   * @param randomIid floating point random value column for output
   * @param nullIid floating point random value column for null percentage
   * @return column with custom random value resolved.
   */
  override def getColumn(randomIid: Column, nullIid: Column): Column = {
    // Pick a random value from (min, max] using randomIid
    val columnExpr = round((randomIid * (max - min)) + min, precision)
    // Add null percentage
    withNull(columnExpr, nullIid, nullPercent)
  }

  /**
   * Get the null percentage value of Fluff Type column
   * @return null percentage
   */
  override def nullPercentage: Int = this.nullPercent

  override def toString: String = s"rangeFluff(min: $min, max: $max, precision: $precision, null%: $nullPercent)"

}

object RangeFluff extends FluffObjectType {
  val NAME_ID: String = "range"

  /**
   * Parser for range function expression
   * @param expr range function expr
   * @param functionDelimiter delimiter for function parameters
   * @return
   */
  def parse(expr: String, functionDelimiter: Char): RangeFluff = {
    // Get range parameters from expr string "range(...)"
    val parsedResult = FunctionParser.parseInputParameters(expr)
    val input: Array[String] = parsedResult._1
      .split(functionDelimiter)
      .map(s => s.trim)

    // If range has only 2 parameters then set min and max value only, else set all values
    if (input.length > 2) {
      new RangeFluff(input(0).toDouble, input(1).toDouble, input(2).toInt, parsedResult._2)
    } else {
      new RangeFluff(input(0).toDouble, input(1).toDouble, nullPercent = parsedResult._2)
    }
  }
}