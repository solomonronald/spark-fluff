package com.solomonronald.spark.fluff.types

import org.apache.spark.sql.Column

trait FluffType extends Serializable {
  def getColumn(c: Column): Column

  val needsRandomIid: Boolean

}
