package com.github.solomonronald.spark.fluff.common

import org.junit.Assert._
import org.scalatest.FunSuite

class FunctionParserTest extends FunSuite {

  test("checkFillPercentagePattern") {
    assertEquals(0 , FunctionParser.parseInputParameters("test(something)[0%]")._2)
    assertEquals(0, FunctionParser.parseInputParameters("test(abcd)")._2)
    assertEquals(0, FunctionParser.parseInputParameters("test(test)[0]")._2)
    assertEquals(10, FunctionParser.parseInputParameters("test(mnop)[10%]")._2)
  }

  test("parseFunctionNameUndefined") {
    assertEquals(Constants.UNDEFINED, FunctionParser.parseFunctionName("(test)"))
  }

  test("parseInputParametersUndefined") {
    assertEquals((Constants.UNDEFINED, Constants.DEFAULT_NULL_PERCENTAGE), FunctionParser.parseInputParameters("(test)"))
  }
}
