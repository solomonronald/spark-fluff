package com.solomonronald.spark.fluff.common

import com.solomonronald.spark.fluff.common.Constants.UNDEFINED

object FunctionParser {
  private final val FUNCTION_EXPR_PATTERN = "^\\w+\\(.*\\)*"

  private def getNullPercentage(expr: String): Int = {
    val defaultPercent: Int = 0
    val defaultPercentString: String = s"[$defaultPercent%]"

    val pattern = "\\[(100|[1-9]?[0-9])\\%\\]$".r
    val percentString = pattern.findFirstIn(expr.trim).getOrElse(defaultPercentString)

    val numberPattern = "\\d+".r
    numberPattern.findFirstIn(percentString).getOrElse(defaultPercent.toString).toInt
  }

  def isValidExpr(expr: String): Boolean = {
    expr.trim.matches(FUNCTION_EXPR_PATTERN)
  }

  def parseFunctionName(expr: String): String = {
    if (isValidExpr(expr)) {
      expr.trim.substring(0, expr.trim.indexOf('('))
    } else {
      UNDEFINED
    }
  }

  def parseInputParameters(expr: String): (String, Int) = {
    val nullPercentage = getNullPercentage(expr)

    if (isValidExpr(expr)) {
      val startingIndex = expr.indexOf('(')
      val endIndex = expr.lastIndexOf(')')
      val params = expr.substring(startingIndex + 1, endIndex)
      (params, nullPercentage)
    } else {
      (UNDEFINED, nullPercentage)
    }
  }
}
