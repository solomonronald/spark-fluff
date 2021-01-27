package com.solomonronald.spark.fluff.types
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.lit

/**
 * [[FluffType]] Function to show constant value in all records of the column.
 * @param const constant value that will be shown in the column
 */
class ConstFluff(val const: String) extends FluffType with Serializable{
  private val serialVersionUID = - 1374449853485783372L
  override val needsRandomIid: Boolean = false

  override def getColumn(c: Column): Column = {
    lit(const)
  }

  override def toString: String = s"constFluff($const)"
}

object ConstFluff extends FluffObjectType {
  override val NAME_ID: String = "cons"

  /**
   * Parser for constant function expression
   * @param expr constant function expr
   * @return
   */
  def parse(expr: String): ConstFluff = {
    // Get constant value inside string "const(...)"
    val input: String = expr.substring(6, expr.length - 1).trim

    new ConstFluff(input)
  }
}
