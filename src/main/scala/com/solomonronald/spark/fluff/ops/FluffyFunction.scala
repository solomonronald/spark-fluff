package com.solomonronald.spark.fluff.ops

import com.solomonronald.spark.fluff.common.Constants.UNDEFINED
import com.solomonronald.spark.fluff.common.FunctionParser
import com.solomonronald.spark.fluff.types._

/**
 * Class to represent Fluff Function
 * @param name Function name
 * @param function [[FluffType]] function
 */
class FluffyFunction(val name: String, val function: FluffType) {
  def this(name: String, functionExpr: String, functionDelimiter: Char) = {
    this(name, FluffyFunction.convertFromExpr(functionExpr, functionDelimiter))
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
   * @param functionDelimiter delimiter for function expression parameters
   * @return
   */
  private def convertFromExpr(expr: String, functionDelimiter: Char): FluffType = {

    val functionName: String = FunctionParser.parseFunctionName(expr)

    functionName match {
      case ListFluff.NAME_ID => ListFluff.parse(expr, functionDelimiter)
      case DateFluff.NAME_ID => DateFluff.parse(expr, functionDelimiter)
      case RangeFluff.NAME_ID => RangeFluff.parse(expr, functionDelimiter)
      case BooleanFluff.NAME_ID => BooleanFluff.parse(expr)
      case UuidFluff.NAME_ID => UuidFluff.parse(expr)
      case ConstFluff.NAME_ID => ConstFluff.parse(expr)
      // Default value is a [[ConstFluff]] of type undefined
      case _ => new ConstFluff(UNDEFINED)
    }
  }
}

