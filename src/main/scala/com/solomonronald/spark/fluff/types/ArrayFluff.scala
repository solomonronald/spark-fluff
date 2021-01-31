package com.solomonronald.spark.fluff.types
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{element_at, lit}
import org.apache.spark.sql.types.IntegerType

/**
 * [[FluffType]] Function to pick an item at random from an array provided by the user.
 * @param arr parsed array of string from which the item will be selected
 */
class ArrayFluff(arr: Array[String]) extends FluffType with Serializable {
  private val serialVersionUID = 8780477305547517901L
  override val needsRandomIid: Boolean = true

  override def getColumn(c: Column): Column = {
    element_at(lit(arr), ((c * arr.length) + 1).cast(IntegerType))
  }

  override def toString: String = s"arrayFluff${arr.mkString("(", ", ", ")")}"
}

object ArrayFluff extends FluffObjectType {
  val NAME_ID: String = "arra"

  /**
   * Parser for array function expression
   * @param expr array function expr
   * @return
   */
  def parse(expr: String, functionDelimiter: Char): ArrayFluff = {
    // Substring ops array from 6th index of string "array(...)"
    val input: Array[String] = expr.substring(6, expr.length - 1)
      .split(functionDelimiter)
      .map(s => s.trim)
      .map(s => if (s.isEmpty) null else s)

    new ArrayFluff(input)
  }
}
