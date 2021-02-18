package com.github.solomonronald.spark.fluff.types

import org.junit.Assert._
import org.scalatest.FunSuite

class RangeFluffTest extends FunSuite {

  test("testToString") {
    val fluffType1: FluffType = new RangeFluff(0, 1, 0)
    assertEquals("rangeFluff(min: 0.0, max: 1.0, precision: 0, null%: 0)", fluffType1.toString)

    val fluffType2: FluffType = new RangeFluff()
    assertEquals("rangeFluff(min: 0.0, max: 1.0, precision: 16, null%: 0)", fluffType2.toString)
  }
}
