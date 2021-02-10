package com.solomonronald.spark.fluff.types

import org.junit.Assert._
import org.scalatest.FunSuite

class UuidFluffTest extends FunSuite {

  test("testToString") {
    val fluffType: FluffType = new UuidFluff()
    assertEquals("uuidFluff(null%: 0)", fluffType.toString)
  }
}
