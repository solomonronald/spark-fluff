package com.solomonronald.spark.fluff.distribution

import org.apache.spark.sql.Column

class RangeDist(min: Double = 0.00, max: Double = 1.00) extends FluffyDistribution {

  override def getColumn(c: Column): Column = {
    (c * ((max - min) + 1)) + min
  }
}
