package com.solomonronald.spark.fluff.util

import com.solomonronald.spark.fluff.distribution.{ArrayDist, DateDist, FluffyDistribution, RangeDist, UuidDist}
import com.solomonronald.spark.fluff.util.FluffyFunction.convertFromExpr

class FluffyFunction(val name: String, val distribution: FluffyDistribution) {
  def this(name: String, functionExpr: String) = this(name, convertFromExpr(functionExpr))

  def toMap: (String, FluffyDistribution) = {
    (name, distribution)
  }

  override def toString: String = {
    s"FluffyFunction{$name, ${distribution.toString}}"
  }
}

object FluffyFunction {
  private def convertFromExpr(expr: String): FluffyDistribution = {
    val functionName: String = expr.substring(0, 4)

    functionName match {
      case ArrayDist.NAME_ID => ArrayDist.parse(expr)
      case DateDist.NAME_ID => DateDist.parse(expr)
      case RangeDist.NAME_ID => RangeDist.parse(expr)
      case UuidDist.NAME_ID => new UuidDist
      case _ => new RangeDist()
    }
  }
}
