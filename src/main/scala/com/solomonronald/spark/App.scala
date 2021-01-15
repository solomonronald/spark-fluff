package com.solomonronald.spark

import org.apache.spark.sql.SparkSession

/**
 *
 */
object App {
  def main(args: Array[String]): Unit = {
    println( "Hello World!" )
    val sparkSession: SparkSession = SparkSession
      .builder()
      .master("local")
      .getOrCreate()

    import sparkSession.implicits._

    val df = Seq(
      (8, "a1"),
      (4, "bcd"),
      (3, "ewq")
    ).toDF("col1", "col2")

    df.show()
  }
}
