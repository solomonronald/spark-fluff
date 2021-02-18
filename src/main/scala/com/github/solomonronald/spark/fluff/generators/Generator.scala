package com.github.solomonronald.spark.fluff.generators

import com.github.solomonronald.spark.fluff.ops.FluffyColumn
import com.github.solomonronald.spark.fluff.types.{ConstFluff, FluffType}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.random.RandomRDDs.uniformVectorRDD
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

object Generator {
  private val DEFAULT_COL_NAME = "defaultCol"

  /**
   * Default fluff value in case function not found in fluffyFunctions map
   */
  lazy val defaultFluff: FluffType = new ConstFluff()

  /**
   * Create a random uniform vector data frame and apply [[FluffType]] functions to create output data frame
   * @param spark SparkSession
   * @param fluffyFunctions Map of function name as key and [[FluffType]] function as value.
   *                        This will be set as a broadcast.
   * @param numRows number of rows for output
   * @param numPartitions number of partitions in the RDD. Default 0 value will set to `sc.defaultParallelism`.
   * @param seed Seed for the RNG that generates the seed for the generator in each partition.
   *             Default 0 value will create a random seed internally.
   * @param columns Array of [[FluffyColumn]]
   * @return
   */
  def randomDf(
                spark: SparkSession,
                fluffyFunctions: Map[String, FluffType],
                numRows: Long,
                numPartitions: Int = 0,
                seed: Long = 0,
                columns: Array[FluffyColumn]
              ): DataFrame = {

    import spark.implicits._

    // Calculate number of columns that require random iid
    val randomIdCols = columns.filter(c => fluffyFunctions.getOrElse(c.functionName, defaultFluff).needsRandomIid)

    // Calculate number of columns that don't have null percentage as 0 or 100
    val nonNullCols = columns.filter(c => needsNullIid(fluffyFunctions.getOrElse(c.functionName, defaultFluff)))

    // Minimum number of random iid columns required to generate data/Only generate for columns that need random iid
    val minColumnRequired: Int = randomIdCols.length + nonNullCols.length

    // Make sure at least 1 column is generated.
    val vectorColumnsLength: Int = if (minColumnRequired > 0) minColumnRequired else 1

    val vectorRdd = if (seed == 0) {
      // Generate random seed if input seed value is 0
      uniformVectorRDD(spark.sparkContext, numRows, vectorColumnsLength, numPartitions)
    } else {
      // Generate with input seed value
      uniformVectorRDD(spark.sparkContext, numRows, vectorColumnsLength, numPartitions, seed)
    }

    // Create data frame from vector rdd map
    val dataFrame: DataFrame = vectorRdd.map(v => v.toArray).toDF(DEFAULT_COL_NAME)

    // Create a broadcast of fluffyFunctions
    val broadcast: Broadcast[Map[String, FluffType]] = spark.sparkContext.broadcast(fluffyFunctions)

    // Function to shorten resolving from broadcast variable
    def getBCastFluff(c: FluffyColumn): FluffType = broadcast.value.getOrElse(c.functionName, defaultFluff)

    // Case 1: Does not need null probability iid and does not need random iid (is static)
    val staticNonNullCols = columns.filter(c => !needsNullIid(getBCastFluff(c)) && !getBCastFluff(c).needsRandomIid)
    // Create expression for case 1
    val staticNonNullColsExpr: Seq[Column] = staticNonNullCols.indices.map(i => {
      val c: FluffyColumn = staticNonNullCols(i)
      // Set null iid column probability as 1 if null percentage is 0
      val nullLitVal = if (getBCastFluff(c).nullPercentage == 0) 1 else 0
      c.resolve(lit(0), lit(nullLitVal), getBCastFluff(c))
    })

    // Case 2: Needs null probability iid but does not need random iid (is static)
    val staticNullableCols = columns.filter(c => needsNullIid(getBCastFluff(c)) && !getBCastFluff(c).needsRandomIid)
    // Create expression for case 2
    val staticNullableColsExpr: Seq[Column] = staticNullableCols.indices.map(i => {
      val c: FluffyColumn = staticNullableCols(i)
      // No delta needed to add to index
      val deltaIndex = i
      // Use the random iid value for null percentage
      c.resolve(lit(0), col(DEFAULT_COL_NAME)(deltaIndex), getBCastFluff(c))
    })

    // Case 3: Does not need null probability iid but need random iid
    val randomNonNullCols = columns.filter(c => !needsNullIid(getBCastFluff(c)) && getBCastFluff(c).needsRandomIid)
    // Create expression for case 3
    val randomNonNullColsExpr: Seq[Column] = randomNonNullCols.indices.map(i => {
      val c: FluffyColumn = randomNonNullCols(i)
      // Add delta from Case 2, i.e. the number of random iid used by case 2
      val deltaIndex =  i + staticNullableCols.length
      // Set null iid column probability as 1 if null percentage is 0
      val nullLitVal = if (getBCastFluff(c).nullPercentage == 0) 1 else 0
      // Use the random iid for generating random value for column
      c.resolve(col(DEFAULT_COL_NAME)(deltaIndex), lit(nullLitVal), getBCastFluff(c))
    })

    // Case 4: Need null probability iid and also random iid
    val randomNullableCols = columns.filter(c => needsNullIid(getBCastFluff(c)) && getBCastFluff(c).needsRandomIid)
    // Create expression for case 4
    val randomNullableColsExpr: Seq[Column] = randomNullableCols.indices.map(i => {
      val c: FluffyColumn = randomNullableCols(i)
      // Add delta from Case 2 + Case 3, i.e. the number of random iid used by case 2 and case 3
      val deltaIndex = (i * 2) + staticNullableCols.length + randomNonNullCols.length
      // Use the random iid for generating random value for column, and +1 random iid index for null percentage
      c.resolve(col(DEFAULT_COL_NAME)(deltaIndex), col(DEFAULT_COL_NAME)(deltaIndex + 1), getBCastFluff(c))
    })

    // Concat all possible column expressions
    val columnExpressions: Seq[Column] = staticNonNullColsExpr ++ staticNullableColsExpr ++
                                         randomNonNullColsExpr ++ randomNullableColsExpr

    // Create an array for ordering by column index
    val orderedColumnNames: Array[String] = columns.map(_.columnName)

    // Convert resulting rdd to dataframe and return
    dataFrame
      // Apply column expression by resolving fluff type for each column value
      .select(columnExpressions: _*)
      // Order the columns based on their index from input
      .select(orderedColumnNames.head, orderedColumnNames.tail: _*)
  }

  /**
   * Returns true if the fluff type needs a random iid to populate null values
   * @param fluffType fluff type
   * @return true if null % is not 0 or 100
   */
  def needsNullIid(fluffType: FluffType): Boolean = {
    // Needs random iid for null if null % is not 0 or 100
    fluffType.nullPercentage != 0 && fluffType.nullPercentage != 100
  }
}
