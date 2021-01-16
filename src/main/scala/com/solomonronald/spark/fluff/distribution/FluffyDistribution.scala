package com.solomonronald.spark.fluff.distribution

import org.apache.spark.sql.Column

trait FluffyDistribution {
  def getColumn(c: Column): Column
}
