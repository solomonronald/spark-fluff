package com.solomonronald.spark.fluff.ops

import com.solomonronald.spark.fluff.types._

/**
 * Class to represent Fluff Function
 * @param name Function name
 * @param function [[FluffType]] function
 */
class FluffyFunction(val name: String, val function: FluffType) {
  def this(name: String, functionExpr: String) = {
    this(name, FluffyFunction.convertFromExpr(functionExpr))
  }

  /**
   * Create [[FluffyFunction]] as a tuple to be converted into a map for broadcasting.
   * @return tuple
   */
  def asMap: (String, FluffType) = {
    (name, function)
  }

  override def toString: String = {
    s"FluffyFunction{$name, ${function.toString}}"
  }
}

object FluffyFunction {
  /**
   * Match/Parse string function expr with respective [[FluffType]]
   * @param expr string function expr
   * @return
   */
  private def convertFromExpr(expr: String): FluffType = {
    // The first 4 characters of the string is the unique NAME_ID of the function
    val functionName: String = expr.substring(0, 4)

    functionName match {
      case ArrayFluff.NAME_ID => ArrayFluff.parse(expr)
      case DateFluff.NAME_ID => DateFluff.parse(expr)
      case RangeFluff.NAME_ID => RangeFluff.parse(expr)
      case UuidFluff.NAME_ID => new UuidFluff
      case ConstFluff.NAME_ID => ConstFluff.parse(expr)
      // Default value is a [[ConstFluff]] of type undefined
      case _ => new ConstFluff("Undefined")
    }
  }
}

