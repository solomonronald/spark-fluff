package com.solomonronald.spark.fluff.types
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{element_at, lit}
import org.apache.spark.sql.types.IntegerType

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

  def parse(expr: String): ArrayFluff = {
    val input: Array[String] = expr.substring(6, expr.length - 1)
      .split(",")
      .map(s => s.trim)
      .map(s => if (s.isEmpty) null else s)

    new ArrayFluff(input)
  }
}
