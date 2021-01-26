package com.solomonronald.spark.fluff.types

import org.junit.Assert._
import org.junit._

class ConstFluffTest {

  @Test
  def testToString(): Unit = {
    val fluffType: FluffType = new ConstFluff("hello")
    assertEquals("constFluff(hello)", fluffType.toString)
  }

}
