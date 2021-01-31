package com.solomonronald.spark.fluff.types
import com.solomonronald.spark.fluff.common.Constants.UNDEFINED
import com.solomonronald.spark.fluff.common.FunctionParser
import com.solomonronald.spark.fluff.common.UtilFunctions.withNull
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.lit

/**
 * [[FluffType]] Function to show constant value in all records of the column.
 * @param const constant value that will be shown in the column
 */
class ConstFluff(val const: String = UNDEFINED, val fillPercent: Int = 100) extends FluffType with Serializable {
  private val serialVersionUID = - 1374449853485783372L
  override val needsRandomIid: Boolean = false

  override def getColumn(c: Column, n: Column): Column = {
    withNull(lit(const), n, fillPercent)
  }

  override def toString: String = s"constFluff($const)"
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
