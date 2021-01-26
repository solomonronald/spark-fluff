package com.solomonronald.spark.fluff.types

import org.junit.Assert._
import org.junit._

@Test
class ArrayFluffTest {

  @Test
  def testToString(): Unit = {
    val fluffType: FluffType = new ArrayFluff(Array[String]("a", "b", "c"))
    assertEquals( "arrayFluff(a, b, c)", fluffType.toString)
  }

}
