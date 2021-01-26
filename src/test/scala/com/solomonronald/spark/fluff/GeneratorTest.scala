package com.solomonronald.spark.fluff

import com.solomonronald.spark.fluff.types.{ConstFluff, FluffType}
import org.apache.spark.sql.DataFrame
import org.junit.Assert._
import org.junit._

@Test
class GeneratorTest extends SharedSparkContext {

  @Test
  def testGenerateByManualInput(): Unit = {
    val columnsInput = Array[FluffyColumn](
      new FluffyColumn(0, "col1", "string", "f1")
    )

    val functionsInput = Array[FluffyFunction](
      new FluffyFunction("f1", new ConstFluff("hello"))
    )

    val functionMap: Map[String, FluffType] = functionsInput.map(_.asMap).toMap

    val df: DataFrame = Generator.randomDf(spark, functionMap, 5,  columns= columnsInput)

    assertEquals("hello", df.distinct().collectAsList().get(0)(0))

    assertEquals(5, df.count())
    assertEquals(1, df.distinct().count())
  }

}