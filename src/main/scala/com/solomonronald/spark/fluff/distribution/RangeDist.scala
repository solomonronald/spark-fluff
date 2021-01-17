package com.solomonronald.spark.fluff.distribution

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.round

class RangeDist(min: Double = 0.00, max: Double = 1.00, precision: Int = 16) extends FluffyDistribution {

  override def getColumn(c: Column): Column = {
    round((c * (max - min)) + min, precision)
  }
}
