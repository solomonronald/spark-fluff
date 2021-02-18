package com.github.solomonronald.spark.fluff.io

import com.github.solomonronald.spark.fluff.common.Constants._
import com.github.solomonronald.spark.fluff.ops.{FluffyColumn, FluffyFunction}
import com.github.solomonronald.spark.fluff.types.FluffType
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}

object FluffyConfigReader {

  /**
   * Schema for functions data frame
   */
  private val FUNCTIONS_SCHEMA = StructType(List(
    StructField(META_COL_FUNCTION_NAME, StringType, nullable = false),
    StructField(META_COL_FUNCTION_EXPR, StringType, nullable = false)
  ))

  /**
   * Schema for columns data frame
   */
  private val COLUMNS_SCHEMA = StructType(List(
    StructField(META_COL_INDEX, IntegerType, nullable = false),
    StructField(META_COL_NAME, StringType, nullable = false),
    StructField(META_COL_TYPE, StringType, nullable = false),
    StructField(META_COL_FUNCTION_EXPR, StringType, nullable = false)
  ))

  /**
   * Create a new column with only function name and generate function name for expr provided.
   */
  private val functionNameCol: Column = {
    val metaExprCol: Column = col(META_COL_FUNCTION_EXPR)
    when(metaExprCol.startsWith("$"), metaExprCol.substr(lit(2), length(metaExprCol)))
      .otherwise(concat(lit("_"), col(META_COL_NAME), col(META_COL_TYPE)))
  }

  /**
   * Read functions csv file
   * @param spark SparkSession
   * @param filePath path for functions csv file
   * @param hasHeader true if csv has header
   * @param delimiter delimiter value for csv file
   * @return functions data frame
   */
  def readFunctions(spark: SparkSession, filePath: String, hasHeader: Boolean, delimiter: String): DataFrame = {
    spark.read
      .option("delimiter", delimiter)
      .option("header", hasHeader)
      .schema(FUNCTIONS_SCHEMA)
      .csv(filePath)
  }

  /**
   * Create an empty function data frame
   * @param spark SparkSession
   * @return Empty function data frame
   */
  def emptyFunctions(spark: SparkSession): DataFrame = {
    spark.createDataFrame(spark.sparkContext.emptyRDD[Row], FUNCTIONS_SCHEMA)
  }

  /**
   * Read columns csv file
   * @param spark SparkSession
   * @param filePath path for columns csv file
   * @param hasHeader true if csv has header
   * @param delimiter delimiter value for csv file
   * @return columns data frame
   */
  def readColumns(spark: SparkSession, filePath: String, hasHeader: Boolean, delimiter: String): DataFrame = {
    spark.read
      .option("delimiter", delimiter)
      .option("header", hasHeader)
      .schema(COLUMNS_SCHEMA)
      .csv(filePath)
      .withColumn(META_COL_FUNCTION_NAME, functionNameCol)
  }

  /**
   * Select function name and function expr from an input of multiple data frames.
   * Converts function expr string to [[FluffType]] function.
   * @param functionDelimiter delimiter for function expression parameters
   * @param dataFrame input data frames
   * @return
   */
  def collectAllFunctions(functionDelimiter: Char, dataFrame: DataFrame*): Array[FluffyFunction] = {
    dataFrame.map(df => {
      df.select(META_COL_FUNCTION_NAME, META_COL_FUNCTION_EXPR)
        .where(not(col(META_COL_FUNCTION_EXPR).startsWith("$")))
    }).reduce(_ union _)
      .collect()
      .map(r => new FluffyFunction(r(0).asInstanceOf[String], r(1).asInstanceOf[String], functionDelimiter))
  }

  /**
   * Convert [[collectAllFunctions]] as map.
   * @param functionDelimiter delimiter for function expression parameters
   * @param dataFrame input data frames
   * @return Map of function name and function of [[FluffType]]
   */
  def collectAllFunctionsAsMap(functionDelimiter: Char, dataFrame: DataFrame*): Map[String, FluffType] = {
    collectAllFunctions(functionDelimiter, dataFrame: _*)
      .map(f => f.asMap)
      .toMap
  }

  /**
   * Collect [[FluffyColumn]] from columns data frame
   * @param dataFrame columns data frame
   * @return
   */
  def collectColumns(dataFrame: DataFrame): Array[FluffyColumn] = {
    dataFrame.select(META_COL_INDEX, META_COL_NAME, META_COL_TYPE, META_COL_FUNCTION_NAME)
      .orderBy(META_COL_INDEX)
      .collect()
      .map(r => new FluffyColumn(r(0).asInstanceOf[Int], r(1).asInstanceOf[String],
        r(2).asInstanceOf[String], r(3).asInstanceOf[String])
      )
  }
}
