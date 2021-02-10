package com.solomonronald.spark.fluff.common

import com.solomonronald.spark.fluff.common.Constants.{DEFAULT_NULL_PERCENTAGE, UNDEFINED}

object FunctionParser {
  private val FUNCTION_EXPR_PATTERN = "^\\w+\\(.*\\)*"

  /**
   * Get null percentage from function expression
   * If no null percentage is available, use default null percentage
   * null percentage is provided after function parameters
   * Ex: functionName(parameters)[nullPercentage%]
   * @param expr
   * @return
   */
  private def getNullPercentage(expr: String): Int = {
    val defaultPercent: Int = DEFAULT_NULL_PERCENTAGE
    val defaultPercentString: String = s"[$defaultPercent%]"

    val pattern = "\\[(100|[1-9]?[0-9])\\%\\]$".r
    val percentString = pattern.findFirstIn(expr.trim).getOrElse(defaultPercentString)

    val numberPattern = "\\d+".r
    numberPattern.findFirstIn(percentString).getOrElse(defaultPercent.toString).toInt
  }

  /**
   * Check if input expression is of valid pattern
   * @param expr function expression
   * @return
   */
  def isValidExpr(expr: String): Boolean = {
    expr.trim.matches(FUNCTION_EXPR_PATTERN)
  }

  /**
   * If the function expression is valid, get the function name from expr.
   * @param expr function expression
   * @return function name, or undefined if expression is invalid
   */
  def parseFunctionName(expr: String): String = {
    if (isValidExpr(expr)) {
      expr.trim.substring(0, expr.trim.indexOf('('))
    } else {
      UNDEFINED
    }
  }

  /**
   * Parse function expression and return a tuple of function parameters and null percentage
   * Format: functionName(function parameters)[nullPercentage%]
   * @param expr function expression
   * @return tuple (function parameters, null percentage)
   */
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
