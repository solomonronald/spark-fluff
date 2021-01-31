package com.solomonronald.spark.fluff.types
import com.solomonronald.spark.fluff.common.FunctionParser
import com.solomonronald.spark.fluff.common.UtilFunctions.withNull
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.expr

/**
 *  [[FluffType]] Function to show a randomly generated UUID.
 */
class UuidFluff(fillPercent: Int = 100) extends FluffType with Serializable {
  private val serialVersionUID = 6195964559328799284L
  override val needsRandomIid: Boolean = false

  override def getColumn(c: Column, n: Column): Column = {
    withNull(expr("uuid()"), n, fillPercent)
  }

  override def toString: String = "uuidFluff()"

}

object UuidFluff extends FluffObjectType {
  val NAME_ID: String = "uuid"

  def parse(expr: String, functionDelimiter: Char): UuidFluff = {
    // Get percentage value for uuid()
    val parsedResult = FunctionParser.parseInputParameters(expr)
    new UuidFluff(parsedResult._2)
  }
}
