package com.solomonronald.spark.fluff.types
import com.solomonronald.spark.fluff.common.Constants.DEFAULT_NULL_PERCENTAGE
import com.solomonronald.spark.fluff.common.FunctionParser
import com.solomonronald.spark.fluff.common.UtilFunctions.withNull
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{element_at, lit}
import org.apache.spark.sql.types.IntegerType

/**
 * [[FluffType]] Function to pick an item at random from an array provided by the user.
 * @param itemList parsed array of string from which the item will be selected
 * @param nullPercent null probability percentage
 */
class ListFluff(itemList: Array[String], nullPercent: Int = DEFAULT_NULL_PERCENTAGE) extends FluffType with Serializable {
  private val serialVersionUID = 8780477305547517901L

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
    // Select a value at random from input list
    val columnExpr = element_at(lit(itemList), ((randomIid * itemList.length) + 1).cast(IntegerType))
    // Add null percentage
    withNull(columnExpr, nullIid, nullPercent)
  }

  /**
   * Get the null percentage value of Fluff Type column
   * @return null percentage
   */
  override def nullPercentage: Int = this.nullPercent

  override def toString: String = s"listFluff(${itemList.mkString("", ", ", "")}, null%: $nullPercent)"
}

object ListFluff extends FluffObjectType {
  val NAME_ID: String = "list"

  /**
   * Parser for array function expression
   * @param expr array function expr
   * @param functionDelimiter delimiter for function parameters
   * @return
   */
  def parse(expr: String, functionDelimiter: Char): ListFluff = {
    // Substring ops array from string "list(...)"
    val parsedResult = FunctionParser.parseInputParameters(expr)
    val input: Array[String] = parsedResult._1
      .split(functionDelimiter)
      .map(s => s.trim)
      .map(s => if (s.isEmpty) null else s)

    new ListFluff(input, parsedResult._2)
  }
}
