package com.solomonronald.spark.fluff.types
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.expr

class UuidFluff extends FluffType with Serializable {
  private val serialVersionUID = 6195964559328799284L
  override val needsRandomIid: Boolean = false

  override def getColumn(c: Column): Column = {
    expr("uuid()")
  }

  override def toString: String = "uuidFluff()"

}

object UuidFluff extends FluffObjectType {
  val NAME_ID: String = "uuid"
}
