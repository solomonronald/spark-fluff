package com.solomonronald.spark.fluff.ops

import org.junit.Assert._
import org.junit._

@Test
class FluffyColumnTest {

  @Test
  def testToString(): Unit = {
    val fluffyColumn: FluffyColumn = new FluffyColumn(0, "col1", "string", "foo")
    assertEquals("FluffyColumn(i: 0, name: col1, type: string, function: foo)", fluffyColumn.toString)
  }

}
