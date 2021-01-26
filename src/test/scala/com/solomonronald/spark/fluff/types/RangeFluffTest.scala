package com.solomonronald.spark.fluff.types

import org.junit.Assert._
import org.junit._

class RangeFluffTest {

  @Test
  def testToString(): Unit = {
    val fluffType: FluffType = new RangeFluff(0, 1, 0)
    assertEquals("rangeFluff(min: 0.0, max: 1.0, precision: 0)", fluffType.toString)
  }
}
