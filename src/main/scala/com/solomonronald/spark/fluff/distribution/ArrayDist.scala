package com.solomonronald.spark.fluff.distribution
import breeze.linalg.max
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{element_at, lit, round}
import org.apache.spark.sql.types.IntegerType

class ArrayDist(arr: Array[String]) extends FluffyDistribution with Serializable {

  override def getColumn(c: Column): Column = {
    element_at(lit(arr), ((c * arr.length) + 1).cast(IntegerType))
  }

  override def toString: String = {
    s"array${arr.mkString("(", ", ", ")")}"
  }
}

object ArrayDist {
  val NAME_ID: String = "arra"

  def parse(expr: String): ArrayDist = {
    val input: Array[String] = expr.substring(6, expr.length - 1)
      .split(",")
      .map(s => s.trim)
      .map(s => if (s.isEmpty) null else s)

    new ArrayDist(input)
  }
}
