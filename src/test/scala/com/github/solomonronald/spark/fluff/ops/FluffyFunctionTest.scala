package com.github.solomonronald.spark.fluff.ops

import com.github.solomonronald.spark.fluff.types.ConstFluff
import com.github.solomonronald.spark.fluff.{Fluff, SharedSparkContext}
import org.apache.spark.sql.DataFrame
import org.junit.Assert._
import org.scalatest.{BeforeAndAfterAll, FunSuite}

class FluffyFunctionTest extends FunSuite with BeforeAndAfterAll with SharedSparkContext {

  override def beforeAll() {
    spark.sparkContext.setLogLevel("ERROR")
  }

  test("testToString") {
    val fluffyFunction: FluffyFunction = new FluffyFunction("f1", new ConstFluff("hello"))
    assertEquals("FluffyFunction{f1, constFluff(hello, null%: 0)}", fluffyFunction.toString)
  }


  test("testGenerateByManualInput") {
    val columnsInput = Array[FluffyColumn](
      new FluffyColumn(0, "col1", "string", "f1")
    )

    val functionsInput = Array[FluffyFunction](
      new FluffyFunction("f1", "xxxx(hello)", ',')
    )

    val df: DataFrame = new Fluff(spark)
      .generate(columnsInput, functionsInput, 5)

    assertEquals("Undefined", df.distinct().collectAsList().get(0)(0))

    assertEquals(5, df.count())
    assertEquals(1, df.distinct().count())
  }

}