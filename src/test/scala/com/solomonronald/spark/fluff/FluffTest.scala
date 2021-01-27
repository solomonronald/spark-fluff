package com.solomonronald.spark.fluff

import com.solomonronald.spark.fluff.TestConstants._
import com.solomonronald.spark.fluff.ops.{FluffyColumn, FluffyFunction}
import com.solomonronald.spark.fluff.types.ConstFluff
import org.apache.spark.sql.DataFrame
import org.junit.Assert._
import org.scalatest.FunSuite

class FluffTest extends FunSuite with SharedSparkContext {

  test("testGenerateByBothFiles") {
    val columnsTestFile: String = getClass.getResource(FILE_COLUMNS_1_CSV).getPath
    val functionsTestFile: String = getClass.getResource(FILE_FUNCTIONS_1_CSV).getPath

    val df: DataFrame = Fluff(spark)
      .generate(columnsCsvPath = columnsTestFile, functionsCsvPath = functionsTestFile, 5)

    assertEquals(5, df.count())
  }

  test("testGenerateByColumnFile") {
    val columnsTestFile: String = getClass.getResource(FILE_COLUMNS_2_CSV).getPath

    val df: DataFrame = Fluff(spark)
      .generate(columnsCsvPath = columnsTestFile, 5)

    assertEquals(5, df.count())
  }


  test("testGenerateByManualInput") {
    val columnsInput = Array[FluffyColumn](
      new FluffyColumn(0, "col1", "string", "f1")
    )

    val functionsInput = Array[FluffyFunction](
      new FluffyFunction("f1", new ConstFluff("hello"))
    )

    val df: DataFrame = new Fluff(spark)
      .generate(columnsInput, functionsInput, 5)

    assertEquals("hello", df.distinct().collectAsList().get(0)(0))
    assertEquals(5, df.count())
    assertEquals(1, df.distinct().count())
  }

  test("testGenerateByManualInput2") {
    val columnsInput = Array[FluffyColumn](
      new FluffyColumn(0, "col1", "string", "f1")
    )

    val functionsInput = Array[FluffyFunction](
      new FluffyFunction("f1", new ConstFluff("hello"))
    )

    val df1: DataFrame = new Fluff(spark)
      .generate(columnsInput, functionsInput, 2)
    assertEquals("hello", df1.distinct().collectAsList().get(0)(0))

    val df2: DataFrame = new Fluff(spark, 1, 1)
      .generate(columnsInput, functionsInput, 2)
    assertEquals("hello", df2.distinct().collectAsList().get(0)(0))

    val df3: DataFrame = Fluff(spark)
      .generate(columnsInput, functionsInput, 2)
    assertEquals("hello", df3.distinct().collectAsList().get(0)(0))

    val df4: DataFrame = Fluff(spark, 1, 1)
      .generate(columnsInput, functionsInput, 2)
    assertEquals("hello", df4.distinct().collectAsList().get(0)(0))
  }


}
