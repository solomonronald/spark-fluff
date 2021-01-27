package com.solomonronald.spark.fluff.ops

import com.solomonronald.spark.fluff.types.FluffType
import org.apache.spark.sql.Column

/**
 * Class to represent a column for Fluff generation.
 *
 * Supported columnType data types are: `string`, `boolean`, `byte`, `short`, `int`, `long`,
 * `float`, `double`, `decimal`, `date`, `timestamp`.
 *
 * @param index to denote the position of column in output
 * @param columnName name of output column
 * @param columnType cast to this data type of output column
 * @param functionName string name of [[FluffyFunction]]
 */
class FluffyColumn(val index: Int,
                   val columnName: String,
                   val columnType: String,
                   val functionName: String) {

  /**
   * Return column with [[columnName]], with [[FluffType]] function of name [[functionName]] applied.
   * And cast to [[columnType]]
   * @param randomValueColumn floating point random value
   * @param fluffType function
   * @return
   */
  def resolve(randomValueColumn: Column, fluffType: FluffType): Column = {
    fluffType.getColumn(randomValueColumn).cast(columnType).as(columnName)
  }

  override def toString: String = {
    s"FluffyColumn(i: $index, name: $columnName, type: $columnType, function: $functionName)"
  }
}
