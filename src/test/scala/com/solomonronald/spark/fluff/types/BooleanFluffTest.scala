package com.solomonronald.spark.fluff.types

import org.junit.Assert.assertEquals
import org.scalatest.FunSuite

class BooleanFluffTest extends FunSuite {

  test("testToString") {
    val fluffType: FluffType = new BooleanFluff()
    assertEquals("booleanFluff(null%: 0)", fluffType.toString)
  }
}
