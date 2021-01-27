package com.solomonronald.spark.fluff.types

import org.junit.Assert._
import org.scalatest.FunSuite

class ArrayFluffTest extends FunSuite {

  test("testToString") {
    val fluffType: FluffType = new ArrayFluff(Array[String]("a", "b", "c"))
    assertEquals( "arrayFluff(a, b, c)", fluffType.toString)
  }

}
