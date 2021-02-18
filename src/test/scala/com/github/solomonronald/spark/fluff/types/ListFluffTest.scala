package com.github.solomonronald.spark.fluff.types

import org.junit.Assert._
import org.scalatest.FunSuite

class ListFluffTest extends FunSuite {

  test("testToString") {
    val fluffType: FluffType = new ListFluff(Array[String]("a", "b", "c"))
    assertEquals( "listFluff(a, b, c, null%: 0)", fluffType.toString)
  }

}
