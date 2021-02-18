package com.github.solomonronald.spark.fluff.types

import org.junit.Assert._
import org.scalatest.FunSuite

class DateFluffTest extends FunSuite {

  test("testToString") {
    val fluffType: FluffType = new DateFluff("2000-01-01", "2030-12-31", "yyyy-MM-dd")
    assertEquals("dateFluff(start: 2000-01-01, end: 2030-12-31, format: yyyy-MM-dd, null%: 0)", fluffType.toString)
  }
}
