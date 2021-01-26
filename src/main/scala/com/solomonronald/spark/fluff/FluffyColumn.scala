package com.solomonronald.spark.fluff

import com.solomonronald.spark.fluff.types.FluffType
import org.apache.spark.sql.Column

class FluffyColumn(val index: Int,
                   val columnName: String,
                   val columnType: String,
                   val functionName: String) {

  def resolve(randomValueColumn: Column, distribution: FluffType): Column = {
    distribution.getColumn(randomValueColumn).cast(columnType).as(columnName)
  }

  override def toString: String = {
    s"FluffyColumn(i: $index, name: $columnName, type: $columnType, function: $functionName)"
  }
}
