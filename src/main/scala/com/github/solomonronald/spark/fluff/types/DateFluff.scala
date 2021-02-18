package com.github.solomonronald.spark.fluff.types
import com.github.solomonronald.spark.fluff.common.Constants.DEFAULT_NULL_PERCENTAGE
import com.github.solomonronald.spark.fluff.common.FunctionParser
import com.github.solomonronald.spark.fluff.common.UtilFunctions.withNull
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{from_unixtime, lit, unix_timestamp}

/**
 * [[FluffType]] Function to pick date at random from a start and end date provided in format.
 *
 * See [[java.text.SimpleDateFormat]] for valid date and time format patterns
 *
 * @param startDateStr Starting date. Inclusive
 * @param endDateStr End date. Exclusive
 * @param format Valid SimpleDateFormat string
 * @param nullPercent null probability percentage
 */
class DateFluff(startDateStr: String,
                endDateStr: String,
                format: String,
                nullPercent: Int = DEFAULT_NULL_PERCENTAGE
               ) extends FluffType with Serializable {
  private val serialVersionUID = 3192225079626485872L

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
    // Convert min time string to time value
    val min = unix_timestamp(lit(startDateStr), format)
    // Convert max time string to time value
    val max = unix_timestamp(lit(endDateStr), format)
    // Get random timestamp
    val timestamp: Column = (randomIid * (max - min)) + min
    // Convert time to required format
    val columnExpr = from_unixtime(timestamp, format)
    // Add null percentage
    withNull(columnExpr, nullIid, nullPercent)
  }

  /**
   * Get the null percentage value of Fluff Type column
   * @return null percentage
   */
  override def nullPercentage: Int = this.nullPercent

  override def toString: String = s"dateFluff(start: $startDateStr, end: $endDateStr, format: $format, null%: $nullPercent)"

}

object DateFluff extends FluffObjectType{
  val NAME_ID: String = "date"

  /**
   * Parser for date function expression
   * @param expr date function expr
   * @param functionDelimiter delimiter for function parameters
   * @return
   */
  def parse(expr: String, functionDelimiter: Char): DateFluff = {
    // Get date parameters from expr string "date(...)"
    val parsedResult = FunctionParser.parseInputParameters(expr)
    val input: Array[String] = parsedResult._1
      .split(functionDelimiter)
      .map(s => s.trim)

    new DateFluff(input(0), input(1), input(2), parsedResult._2)
  }
}
