package com.solomonronald.spark.fluff.types
import com.solomonronald.spark.fluff.common.Constants.DEFAULT_NULL_PERCENTAGE
import com.solomonronald.spark.fluff.common.FunctionParser
import org.apache.spark.sql.Column
import org.apache.spark.sql.types.BooleanType

class BooleanFluff(nullPercent: Int = DEFAULT_NULL_PERCENTAGE) extends FluffType with Serializable {
  private val serialVersionUID = - 4893248910189155152L
  override val needsRandomIid: Boolean = true
  private val listFluff: ListFluff = new ListFluff(Array[String]("true", "false"), nullPercent)

  override def getColumn(c: Column, n: Column): Column = {
    listFluff.getColumn(c, n).cast(BooleanType)
  }
}

object BooleanFluff extends FluffObjectType {
  override val NAME_ID: String = "bool"

  def parse(expr: String): BooleanFluff = {
    val parsedResult = FunctionParser.parseInputParameters(expr)

    new BooleanFluff(parsedResult._2)
  }

}
