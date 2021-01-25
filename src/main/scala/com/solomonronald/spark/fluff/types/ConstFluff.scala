package com.solomonronald.spark.fluff.types
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.lit

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

  def parse(expr: String): ConstFluff = {
    val input: String = expr.substring(6, expr.length - 1).trim

    new ConstFluff(input)
  }
}
