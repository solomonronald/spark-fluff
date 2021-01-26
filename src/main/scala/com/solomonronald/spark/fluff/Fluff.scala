package com.solomonronald.spark.fluff

import com.solomonronald.spark.fluff.generators.Generator
import com.solomonronald.spark.fluff.io.FluffyConfigReader._
import com.solomonronald.spark.fluff.ops.{FluffyColumn, FluffyFunction}
import com.solomonronald.spark.fluff.types.FluffType
import org.apache.spark.sql.{DataFrame, SparkSession}

class Fluff(spark: SparkSession, val numPartitions: Int = 0, val seed: Long = 0) {

  def generate(targetCols: Array[FluffyColumn], fluffyFunctions: Array[FluffyFunction], numRows: Long): DataFrame = {
    val functionMap: Map[String, FluffType] = fluffyFunctions.map(_.asMap).toMap
    Generator.randomDf(spark, functionMap, numRows, numPartitions, seed, targetCols)
  }

  def generate(columnsCsvPath: String, numRows: Long): DataFrame = {
    val columnsDf: DataFrame = readColumns(spark, columnsCsvPath)
    val functionsDf: DataFrame = emptyFunctions(spark)

    generate(
      columnsDf = columnsDf,
      functionsDf = functionsDf,
      numRows = numRows)
  }

  def generate(columnsCsvPath: String, functionsCsvPath: String, numRows: Long): DataFrame = {

    val columnsDf: DataFrame = readColumns(spark, columnsCsvPath)
    val functionsDf: DataFrame = readFunctions(spark, functionsCsvPath)

    generate(
      columnsDf = columnsDf,
      functionsDf = functionsDf,
      numRows = numRows)
  }

  private def generate(columnsDf: DataFrame, functionsDf: DataFrame, numRows: Long): DataFrame = {
    val functionMap: Map[String, FluffType] = collectAllFunctionsAsMap(columnsDf, functionsDf)
    val targetCols: Array[FluffyColumn] = collectColumns(columnsDf)
    Generator.randomDf(spark, functionMap, numRows, numPartitions, seed, targetCols)
  }
}

object Fluff {
  def apply(spark: SparkSession, numPartitions: Int = 0, seed: Long = 0): Fluff = {
    new Fluff(spark, numPartitions, seed)
  }
}
