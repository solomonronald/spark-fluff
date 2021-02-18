package com.github.solomonronald.spark.fluff.generators

import com.github.solomonronald.spark.fluff.SharedSparkContext
import com.github.solomonronald.spark.fluff.ops.{FluffyColumn, FluffyFunction}
import com.github.solomonronald.spark.fluff.types.{ConstFluff, FluffType}
import org.apache.spark.sql.DataFrame
import org.junit.Assert._
import org.scalatest.{BeforeAndAfterAll, FunSuite}

class GeneratorTest extends FunSuite with BeforeAndAfterAll with SharedSparkContext {

  override def beforeAll() {
    spark.sparkContext.setLogLevel("ERROR")
  }

  test("defaultFluff") {
    val fluffType: FluffType = Generator.defaultFluff
    assertEquals("constFluff(Undefined, null%: 0)", fluffType.toString)
  }

  test("testGenerateByManualInput") {
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