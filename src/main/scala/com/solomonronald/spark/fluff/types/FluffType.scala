package com.solomonronald.spark.fluff.types

import org.apache.spark.sql.Column

/**
 * Base trait for Fluff functions
 */
trait FluffType extends Serializable {
  /**
   * Spark column expression to generate custom random column value.
   * @param c floating point random value column for output
   * @param n floating point random value column for null percentage
   * @return column with custom random value resolved.
   */
  def getColumn(c: Column, n: Column): Column

  /**
   * Set this to true if the FluffType requires ops floating point random value.
   */
  val needsRandomIid: Boolean

}
