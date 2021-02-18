package com.github.solomonronald.spark.fluff.types
import com.github.solomonronald.spark.fluff.common.Constants.DEFAULT_NULL_PERCENTAGE
import com.github.solomonronald.spark.fluff.common.FunctionParser
import org.apache.spark.sql.Column
import org.apache.spark.sql.types.BooleanType

/**
 * [[FluffType]] Function to set true or false.
 * @param nullPercent null probability percentage
 */
class BooleanFluff(nullPercent: Int = DEFAULT_NULL_PERCENTAGE) extends FluffType with Serializable {
  private val serialVersionUID = - 4893248910189155152L

  /**
   * This fluff requires a random iid
   */
  override val needsRandomIid: Boolean = true
  private val listFluff: ListFluff = new ListFluff(Array[String]("true", "false"), nullPercent)

  /**
   * Spark column expression to generate custom random column value.
   * @param randomIid floating point random value column for output
   * @param nullIid floating point random value column for null percentage
   * @return column with custom random value resolved.
   */
  override def getColumn(randomIid: Column, nullIid: Column): Column = {
    // Boolean Fluff is logically a ListFluff with [true, false] as list items
    listFluff.getColumn(randomIid, nullIid).cast(BooleanType)
  }

  /**
   * Get the null percentage value of Fluff Type column
   * @return null percentage
   */
  override def nullPercentage: Int = listFluff.nullPercentage

  override def toString = s"booleanFluff(null%: $nullPercent)"
}

object BooleanFluff extends FluffObjectType {
  override val NAME_ID: String = "bool"

  /**
   * Parser for Boolean Function
   * @param expr function expression
   * @return
   */
  def parse(expr: String): BooleanFluff = {
    val parsedResult = FunctionParser.parseInputParameters(expr)
    new BooleanFluff(parsedResult._2)
  }

}
