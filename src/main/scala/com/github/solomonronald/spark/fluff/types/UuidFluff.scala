package com.github.solomonronald.spark.fluff.types
import com.github.solomonronald.spark.fluff.common.Constants.DEFAULT_NULL_PERCENTAGE
import com.github.solomonronald.spark.fluff.common.FunctionParser
import com.github.solomonronald.spark.fluff.common.UtilFunctions.withNull
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.expr

/**
 * [[FluffType]] Function to show a randomly generated UUID.
 * @param nullPercent null probability percentage
 */
class UuidFluff(nullPercent: Int = DEFAULT_NULL_PERCENTAGE) extends FluffType with Serializable {
  private val serialVersionUID = 6195964559328799284L

  /**
   * This fluff does not need a random iid
   */
  override val needsRandomIid: Boolean = false

  /**
   * Spark column expression to generate custom random column value.
   * @param randomIid floating point random value column for output
   * @param nullIid floating point random value column for null percentage
   * @return column with custom random value resolved.
   */
  override def getColumn(randomIid: Column, nullIid: Column): Column = {
    // Random IID value is not required as we'll leverage spark's in build uuid function
    val columnExpr = expr("uuid()")
    // Add null percentage
    withNull(columnExpr, nullIid, nullPercent)
  }

  /**
   * Get the null percentage value of Fluff Type column
   * @return null percentage
   */
  override def nullPercentage: Int = this.nullPercent

  override def toString: String = s"uuidFluff(null%: $nullPercent)"

}

object UuidFluff extends FluffObjectType {
  val NAME_ID: String = "uuid"

  /**
   * Parser for UUID function
   * @param expr function expression
   * @return
   */
  def parse(expr: String): UuidFluff = {
    // Get percentage value for uuid()
    val parsedResult = FunctionParser.parseInputParameters(expr)
    new UuidFluff(parsedResult._2)
  }
}
