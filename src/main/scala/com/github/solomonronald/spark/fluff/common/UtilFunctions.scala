package com.github.solomonronald.spark.fluff.common

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{lit, when}
import org.apache.spark.sql.types.DoubleType

object UtilFunctions {

  /**
   * Adds null values to a column based on null percentage
   * @param randomIid random value column for data
   * @param nullIid random value column for null items
   * @param nullPercentage null percentage
   * @return resolved column
   */
  def withNull(randomIid: Column, nullIid: Column, nullPercentage: Int): Column = {
    // If null column value is greater than null percentage, return random iid value
    when((nullIid * 100).cast(DoubleType) >= lit(nullPercentage).cast(DoubleType), randomIid)
      // Else return null value
      .otherwise(lit(null))
  }
}
