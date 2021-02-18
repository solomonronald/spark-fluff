package com.github.solomonronald.spark.fluff

import org.apache.spark.sql.SparkSession

trait SharedSparkContext {
  lazy val spark: SparkSession = {
    SparkSession
      .builder()
      .appName("FluffTest")
      .master("local[*]")
      .getOrCreate()
  }

}
