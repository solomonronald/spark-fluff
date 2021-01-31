package com.solomonronald.spark.fluff.generators

import com.solomonronald.spark.fluff.ops.FluffyColumn
import com.solomonronald.spark.fluff.types.{ConstFluff, FluffType}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.random.RandomRDDs.uniformVectorRDD
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

object Generator {
  private val DEFAULT_COL_NAME = "defaultCol"

  /**
   * Create a random uniform vector data frame and apply [[FluffType]] functions to create output data frame
   * @param spark SparkSession
   * @param fluffyFunctions Map of function name as key and [[FluffType]] function as value.
   *                        This will be set as a broadcast.
   * @param numRows number of rows for output
   * @param numPartitions number of partitions in the RDD. Default 0 value will set to `sc.defaultParallelism`.
   * @param seed Seed for the RNG that generates the seed for the generator in each partition.
   *             Default 0 value will create a random seed internally.
   * @param columns Array of [[FluffyColumn]]
   * @return
   */
  def randomDf(
                spark: SparkSession,
                fluffyFunctions: Map[String, FluffType],
                numRows: Long,
                numPartitions: Int = 0,
                seed: Long = 0,
                columns: Array[FluffyColumn]
              ): DataFrame = {

    import spark.implicits._

    // x2 the column length, 1 for column value, 1 for null percentage
    val vectorColumnsLength = columns.length * 2

    val vectorRdd = if (seed == 0) {
      // Generate random seed if input seed value is 0
      uniformVectorRDD(spark.sparkContext, numRows, vectorColumnsLength, numPartitions)
    } else {
      // Generate with input seed value
      uniformVectorRDD(spark.sparkContext, numRows, vectorColumnsLength, numPartitions, seed)
    }

    val rdd = vectorRdd.map(v => v.toArray)

    // Default fluff value in case function not found in fluffyFunctions map
    val defaultFluff: FluffType = new ConstFluff()

    // Create a broadcast of fluffyFunctions
    val functionBroadcast: Broadcast[Map[String, FluffType]] = spark.sparkContext.broadcast(fluffyFunctions)

    // Resolve FluffyColumn with FluffType function
    val columnExpressions: Seq[Column] = columns.indices.map(i => {
      val c = columns(i)
      val colIndex: Int = i * 2
      c.resolve(
        col(DEFAULT_COL_NAME)(colIndex),
        col(DEFAULT_COL_NAME)(colIndex + 1),
        functionBroadcast.value.getOrElse(c.functionName, defaultFluff)
      )
    })

    // Convert resulting rdd to dataframe and return
    rdd.toDF(DEFAULT_COL_NAME).select(columnExpressions: _*)
  }
}
