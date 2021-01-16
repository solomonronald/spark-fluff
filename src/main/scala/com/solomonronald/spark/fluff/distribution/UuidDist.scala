package com.solomonronald.spark.fluff.distribution
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.expr

class UuidDist extends FluffyDistribution {
  override def getColumn(c: Column): Column = {
    expr("uuid()")
  }
}
