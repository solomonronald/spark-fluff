package com.solomonronald.spark.fluff

import com.solomonronald.spark.fluff.types._

class FluffyFunction(val name: String, val function: FluffType) {
  def this(name: String, functionExpr: String) = {
    this(name, FluffyFunction.convertFromExpr(functionExpr))
  }

  def asMap: (String, FluffType) = {
    (name, function)
  }

  override def toString: String = {
    s"FluffyFunction{$name, ${function.toString}}"
  }
}

object FluffyFunction {
  private def convertFromExpr(expr: String): FluffType = {
    val functionName: String = expr.substring(0, 4)

    functionName match {
      case ArrayFluff.NAME_ID => ArrayFluff.parse(expr)
      case DateFluff.NAME_ID => DateFluff.parse(expr)
      case RangeFluff.NAME_ID => RangeFluff.parse(expr)
      case UuidFluff.NAME_ID => new UuidFluff
      case ConstFluff.NAME_ID => ConstFluff.parse(expr)
      case _ => new ConstFluff("Undefined")
    }
  }
}

