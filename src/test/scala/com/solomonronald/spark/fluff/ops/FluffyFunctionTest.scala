package com.solomonronald.spark.fluff.ops

import com.solomonronald.spark.fluff.types.ConstFluff
import com.solomonronald.spark.fluff.{Fluff, SharedSparkContext}
import org.apache.spark.sql.DataFrame
import org.junit.Assert._
import org.scalatest.FunSuite

class FluffyFunctionTest extends FunSuite with SharedSparkContext {

  test("testToString") {
    val fluffyFunction: FluffyFunction = new FluffyFunction("f1", new ConstFluff("hello"))
    assertEquals("FluffyFunction{f1, constFluff(hello)}", fluffyFunction.toString)
  }


  test("testGenerateByManualInput") {
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