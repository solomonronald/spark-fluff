package com.solomonronald.spark.fluff.common

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{lit, when}
import org.apache.spark.sql.types.DoubleType

object UtilFunctions {

  /**
   * Adds null values to a column based on null percentage
   * @param c random value column for data
   * @param n random value column for null items
   * @param nullPercentage null percentage
   * @return resolved column
   */
  def withNull(c: Column, n: Column, nullPercentage: Int): Column = {
    // If null column value is greater than null percentage, return column value
    when((n * 100).cast(DoubleType) >= lit(nullPercentage).cast(DoubleType), c)
      // Else return null value
      .otherwise(lit(null))
  }
}
