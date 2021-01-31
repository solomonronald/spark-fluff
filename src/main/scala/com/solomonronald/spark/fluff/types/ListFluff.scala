package com.solomonronald.spark.fluff.types
import com.solomonronald.spark.fluff.common.FunctionParser
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{element_at, lit}
import org.apache.spark.sql.types.IntegerType

/**
 * [[FluffType]] Function to pick an item at random from an array provided by the user.
 * @param arr parsed array of string from which the item will be selected
 */
class ListFluff(arr: Array[String], fillPercent: Int = 100) extends FluffType with Serializable {
  private val serialVersionUID = 8780477305547517901L
  override val needsRandomIid: Boolean = true

  override def getColumn(c: Column): Column = {
    element_at(lit(arr), ((c * arr.length) + 1).cast(IntegerType))
  }

  override def toString: String = s"listFluff${arr.mkString("(", ", ", ")")}"
}

object ListFluff extends FluffObjectType {
  val NAME_ID: String = "list"

  /**
   * Parser for array function expression
   * @param expr array function expr
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
