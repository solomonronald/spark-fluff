package com.solomonronald.spark.fluff

import com.solomonronald.spark.fluff.generators.Generator
import com.solomonronald.spark.fluff.io.FluffyConfigReader._
import com.solomonronald.spark.fluff.ops.{FluffyColumn, FluffyFunction}
import com.solomonronald.spark.fluff.types.FluffType
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Creates an object to initialize random data generation
 * @param spark Spark Session
 * @param numPartitions number of partitions in the RDD
 * @param seed Seed for the RNG that generates the seed for the generator in each partition.
 * @param hasHeader Set it to false if the csv file is missing header
 * @param fileDelimiter Delimiter for csv file
 * @param functionDelimiter Delimiter for separating parameters in function expression
 */
class Fluff(
             spark: SparkSession,
             val numPartitions: Int = 0,
             val seed: Long = 0,
             val hasHeader: Boolean = true,
             val fileDelimiter: String = ",",
             val functionDelimiter: Char = '|'
           ) {

  /**
   * Generate random data frame using manual column and function description
   * @param targetCols array of [[FluffyColumn]] representing target columns
   * @param fluffyFunctions array of [[FluffyFunction]]
   * @param numRows number of output rows
   * @return data frame with random values
   */
  def generate(targetCols: Array[FluffyColumn], fluffyFunctions: Array[FluffyFunction], numRows: Long): DataFrame = {
    val functionMap: Map[String, FluffType] = fluffyFunctions.map(_.asMap).toMap
    Generator.randomDf(spark, functionMap, numRows, numPartitions, seed, targetCols)
  }

  /**
   * Generate random data frame using only csv file for columns
   * @param columnsCsvPath path for csv file describing target columns
   * @param numRows number of output rows
   * @return
   */
  def generate(columnsCsvPath: String, numRows: Long): DataFrame = {
    val columnsDf: DataFrame = readColumns(spark, columnsCsvPath, hasHeader, fileDelimiter)
    val functionsDf: DataFrame = emptyFunctions(spark)

    generate(
      columnsDf = columnsDf,
      functionsDf = functionsDf,
      numRows = numRows)
  }

  /**
   * Generate random data frame using csv file for columns and functions
   * @param columnsCsvPath path for csv file describing target columns
   * @param functionsCsvPath path for csv file describing column functions
   * @param numRows number of output rows
   * @return
   */
  def generate(columnsCsvPath: String, functionsCsvPath: String, numRows: Long): DataFrame = {

    val columnsDf: DataFrame = readColumns(spark, columnsCsvPath, hasHeader, fileDelimiter)
    val functionsDf: DataFrame = readFunctions(spark, functionsCsvPath, hasHeader, fileDelimiter)

    generate(
      columnsDf = columnsDf,
      functionsDf = functionsDf,
      numRows = numRows)
  }

  /**
   * Generate random data frame using columns and function data frame
   * @param columnsDf data frame describing target columns
   * @param functionsDf data frame describing functions
   * @param numRows number of output rows
   * @return
   */
  private def generate(columnsDf: DataFrame, functionsDf: DataFrame, numRows: Long): DataFrame = {
    val functionMap: Map[String, FluffType] = collectAllFunctionsAsMap(functionDelimiter, columnsDf, functionsDf)
    val targetCols: Array[FluffyColumn] = collectColumns(columnsDf)
    Generator.randomDf(spark, functionMap, numRows, numPartitions, seed, targetCols)
  }
}

object Fluff {
  /**
   * Creates an object to initialize random data generation
   * @param spark Spark Session
   * @param numPartitions number of partitions in the RDD
   * @param seed Seed for the RNG that generates the seed for the generator in each partition.
   * @param hasHeader Set it to false if the csv file is missing header
   * @param fileDelimiter Delimiter for csv file
   * @param functionDelimiter Delimiter for separating parameters in function expression
   */
  def apply(
             spark: SparkSession,
             numPartitions: Int = 0,
             seed: Long = 0,
             hasHeader: Boolean = true,
             fileDelimiter: String = ",",
             functionDelimiter: Char = '|'
           ): Fluff = {
    new Fluff(spark, numPartitions, seed, hasHeader, fileDelimiter, functionDelimiter)
  }
}
