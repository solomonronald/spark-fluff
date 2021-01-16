package com.solomonronald.spark.fluff

import com.solomonronald.spark.fluff.distribution.{FluffyDistribution, RangeDist}
import org.apache.spark.sql.Column

class FluffyColumn(
                    val name: String,
                    val dataType: String = "double",
                    val distribution: FluffyDistribution = new RangeDist
                  ) {
  def resolve(c: Column): Column = {
    distribution.getColumn(c).cast(dataType).as(name)
  }
}
