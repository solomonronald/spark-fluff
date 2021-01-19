package com.solomonronald.spark.fluff.distribution
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.expr

class UuidDist extends FluffyDistribution with Serializable {

  override def getColumn(c: Column): Column = {
    expr("uuid()")
  }

  override def toString: String = {
    "uuid()"
  }
}

object UuidDist {
  val NAME_ID: String = "uuid"
}
