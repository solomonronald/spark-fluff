package com.github.solomonronald.spark.fluff.types

import org.apache.spark.sql.Column

/**
 * Base trait for Fluff functions
 */
trait FluffType extends Serializable {
  /**
   * Spark column expression to generate custom random column value.
   * @param randomIid floating point random value column for output
   * @param nullIid floating point random value column for null percentage
   * @return column with custom random value resolved.
   */
  def getColumn(randomIid: Column, nullIid: Column): Column

  /**
   * Set this to true if the FluffType requires ops floating point random value.
   */
  val needsRandomIid: Boolean

  /**
   * Get the null percentage value of Fluff Type column
   * @return null percentage
   */
  def nullPercentage: Int

}
