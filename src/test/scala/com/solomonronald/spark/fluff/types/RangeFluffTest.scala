package com.solomonronald.spark.fluff.types

import org.junit.Assert._
import org.scalatest.FunSuite

class RangeFluffTest extends FunSuite {

  test("testToString") {
    val fluffType: FluffType = new RangeFluff(0, 1, 0)
    assertEquals("rangeFluff(min: 0.0, max: 1.0, precision: 0)", fluffType.toString)
  }
}
