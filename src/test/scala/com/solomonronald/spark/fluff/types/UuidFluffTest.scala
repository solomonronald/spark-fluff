package com.solomonronald.spark.fluff.types

import org.junit.Assert._
import org.junit._

class UuidFluffTest {

  @Test
  def testToString(): Unit = {
    val fluffType: FluffType = new UuidFluff()
    assertEquals("uuidFluff()", fluffType.toString)
  }
}
