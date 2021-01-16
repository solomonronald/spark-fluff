package com.solomonronald.spark.fluff

import org.apache.spark.mllib.random.RandomRDDs._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

object RandomGenerator {
  private val DEFAULT_COL_NAME = "defaultCol"

  def uniformDf(
               spark: SparkSession,
               numRows: Long,
               numPartitions: Int = 0,
               seed: Long = 0,
               columns: Array[FluffyColumn]
               ): DataFrame = {

    import spark.implicits._

    val vectorRdd = if (seed == 0) {
      uniformVectorRDD(spark.sparkContext, numRows, columns.length, numPartitions)
    } else {
      uniformVectorRDD(spark.sparkContext, numRows, columns.length, numPartitions, seed)
    }

    val rdd = vectorRdd.map(v => v.toArray)
    val columnExpressions: Seq[Column] = columns.indices.map(i => columns(i).resolve(col(DEFAULT_COL_NAME)(i)))

    rdd.toDF(DEFAULT_COL_NAME).select(columnExpressions: _*)
  }
}
