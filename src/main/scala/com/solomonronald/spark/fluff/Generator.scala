package com.solomonronald.spark.fluff

import com.solomonronald.spark.fluff.types.{FluffType, RangeFluff}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.random.RandomRDDs._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

object Generator {
  private val DEFAULT_COL_NAME = "defaultCol"

  def randomDf(
                spark: SparkSession,
                fluffyFunctions: Map[String, FluffType],
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
    val rangeDist: FluffType = new RangeFluff()

    val functionBroadcast: Broadcast[Map[String, FluffType]] = spark.sparkContext.broadcast(fluffyFunctions)

    val columnExpressions: Seq[Column] = columns.indices.map(i => {
      val c = columns(i)
      c.resolve(col(DEFAULT_COL_NAME)(i), functionBroadcast.value.getOrElse(c.functionName, rangeDist))
    })

    rdd.toDF(DEFAULT_COL_NAME).select(columnExpressions: _*)
  }
}
