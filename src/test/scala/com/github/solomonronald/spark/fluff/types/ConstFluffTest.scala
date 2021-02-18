package com.github.solomonronald.spark.fluff.types

import org.junit.Assert._
import org.scalatest.FunSuite

class ConstFluffTest extends FunSuite {

  test("testToString") {
    val fluffType: FluffType = new ConstFluff("hello")
    assertEquals("constFluff(hello, null%: 0)", fluffType.toString)
  }

}
