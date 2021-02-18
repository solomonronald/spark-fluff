package com.github.solomonronald.spark.fluff.types

/**
 * Trait for FluffType implementation object.
 */
trait FluffObjectType {
  /**
   * NAME_ID is a 4 character string to uniquely identify the FluffType and parse from ops expression.
   */
  val NAME_ID: String
}
