package com.solomonronald.spark.fluff

import com.solomonronald.spark.fluff.types.ConstFluff
import org.apache.spark.sql.DataFrame
import org.junit.Assert._
import org.junit._

@Test
class FluffyFunctionTest extends SharedSparkContext {

  @Test
  def testToString(): Unit = {
    val fluffyFunction: FluffyFunction = new FluffyFunction("f1", new ConstFluff("hello"))
    assertEquals("FluffyFunction{f1, constFluff(hello)}", fluffyFunction.toString)
  }

  @Test
  def testGenerateByManualInput(): Unit = {
    val columnsInput = Array[FluffyColumn](
      new FluffyColumn(0, "col1", "string", "f1")
    )

    val functionsInput = Array[FluffyFunction](
      new FluffyFunction("f1", "xxxx(hello)")
    )

    val df: DataFrame = new Fluff(spark)
      .generate(columnsInput, functionsInput, 5)

    assertEquals("Undefined", df.distinct().collectAsList().get(0)(0))

    assertEquals(5, df.count())
    assertEquals(1, df.distinct().count())
  }

}