package com.solomonronald.spark

import org.apache.spark.sql.SparkSession

trait SharedSparkContext {
  lazy val spark: SparkSession = {
    SparkSession
      .builder()
      .appName("Fluff Test")
      .master("local[*]")
      .getOrCreate()
  }

}
