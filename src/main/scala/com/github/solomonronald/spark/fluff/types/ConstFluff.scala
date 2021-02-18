package com.github.solomonronald.spark.fluff.types
import com.github.solomonronald.spark.fluff.common.Constants.{DEFAULT_NULL_PERCENTAGE, UNDEFINED}
import com.github.solomonronald.spark.fluff.common.FunctionParser
import com.github.solomonronald.spark.fluff.common.UtilFunctions.withNull
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.lit

/**
 * [[FluffType]] Function to show constant value in all records of the column.
 * @param const constant value that will be shown in the column
 * @param nullPercent null probability percentage
 */
class ConstFluff(val const: String = UNDEFINED, val nullPercent: Int = DEFAULT_NULL_PERCENTAGE) extends FluffType with Serializable {
  private val serialVersionUID = - 1374449853485783372L

  /**
   * This fluff does not need a random iid
   */
  override val needsRandomIid: Boolean = false

  /**
   * Spark column expression to generate custom random column value.
   * @param randomIid floating point random value column for output
   * @param nullIid floating point random value column for null percentage
   * @return column with custom random value resolved.
   */
  override def getColumn(randomIid: Column, nullIid: Column): Column = {
    // Random iid not required for constant literal
    withNull(lit(const), nullIid, nullPercent)
  }

  /**
   * Get the null percentage value of Fluff Type column
   * @return null percentage
   */
  override def nullPercentage: Int = this.nullPercent

  override def toString: String = s"constFluff($const, null%: $nullPercent)"
}

object ConstFluff extends FluffObjectType {
  override val NAME_ID: String = "const"

  /**
   * Parser for constant function expression
   * @param expr constant function expr
   * @return
   */
  def parse(expr: String): ConstFluff = {
    // Get constant value inside string "const(...)"
    val parsedResult = FunctionParser.parseInputParameters(expr)
    val input: String = parsedResult._1.trim

    new ConstFluff(input, parsedResult._2)
  }
}
