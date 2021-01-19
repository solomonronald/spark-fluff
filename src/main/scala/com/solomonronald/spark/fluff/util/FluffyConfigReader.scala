package com.solomonronald.spark.fluff.util

import com.solomonronald.spark.fluff.FluffyColumn
import com.solomonronald.spark.fluff.distribution.FluffyDistribution
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

object FluffyConfigReader {
  private val META_COL_FUNCTION_EXPR = "functionExpr"
  private val META_COL_FUNCTION_NAME = "functionName"
  private val META_COL_TYPE = "type"
  private val META_COL_NAME = "name"
  private val META_COL_INDEX = "index"

  private val FUNCTIONS_SCHEMA = StructType(Array(
    StructField(META_COL_FUNCTION_NAME, StringType, nullable = false),
    StructField(META_COL_FUNCTION_EXPR, StringType, nullable = false),
  ))

  private val COLUMNS_SCHEMA = StructType(Array(
    StructField(META_COL_INDEX, IntegerType, nullable = false),
    StructField(META_COL_NAME, StringType, nullable = false),
    StructField(META_COL_TYPE, StringType, nullable = false),
    StructField(META_COL_FUNCTION_EXPR, StringType, nullable = false)
  ))

  private val DELIMITER = "|"

  def readFunctions(spark: SparkSession, filePath: String): DataFrame = {
    spark.read
      .option("delimiter", DELIMITER)
      .schema(FUNCTIONS_SCHEMA)
      .csv(filePath)
  }

  def readColumns(spark: SparkSession, filePath: String): DataFrame = {
    spark.read
      .option("delimiter", DELIMITER)
      .schema(COLUMNS_SCHEMA)
      .csv(filePath)
      .withColumn(META_COL_FUNCTION_NAME, functionNameCol)
  }

  private val functionNameCol: Column = {
    val metaExprCol: Column = col(META_COL_FUNCTION_EXPR)
    when(metaExprCol.startsWith("$"), metaExprCol.substr(lit(2), length(metaExprCol)))
      .otherwise(concat(lit("_"), col(META_COL_NAME), col(META_COL_TYPE)))
  }

  def collectAllFunctions(dataFrame: DataFrame*): Map[String, FluffyDistribution] = {
    dataFrame.map(df => {
      df.select(META_COL_FUNCTION_NAME, META_COL_FUNCTION_EXPR)
        .where(not(col(META_COL_FUNCTION_EXPR).startsWith("$")))
    }).reduce(_ union _)
      .collect()
      .map(r => new FluffyFunction(r(0).asInstanceOf[String], r(1).asInstanceOf[String]).toMap)
      .toMap
  }

  def collectColumns(dataFrame: DataFrame): Array[FluffyColumn] = {
    dataFrame.select(META_COL_INDEX, META_COL_NAME, META_COL_TYPE, META_COL_FUNCTION_NAME)
      .orderBy(META_COL_INDEX)
      .collect()
      .map(r => new FluffyColumn(r(0).asInstanceOf[Int], r(1).asInstanceOf[String],
        r(2).asInstanceOf[String], r(3).asInstanceOf[String])
      )
  }
}
