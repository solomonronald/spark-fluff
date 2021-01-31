package com.solomonronald.spark.fluff.common

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{lit, when}
import org.apache.spark.sql.types.DoubleType

object UtilFunctions {
  def withNull(c: Column, n: Column, nullPercentage: Int): Column = {
    when((n * 100).cast(DoubleType) >= lit(nullPercentage).cast(DoubleType), c).otherwise(lit(null))
  }
}
