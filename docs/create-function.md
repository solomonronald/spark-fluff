# Create a custom Fluff Function

If you find that the [Fluff Functions](./fluff-functions.md) currently implented in _Fluff_ is missing some functionality then you can create your own custom Fluff Function that converts  the i.i.d's provided by [Spark MLlib's RandomRDDs](https://spark.apache.org/docs/3.0.0-preview/api/scala/org/apache/spark/mllib/random/RandomRDDs$.html) into required random data.

Following are the steps to create a new Fluff Function:

## Step 1: Create a new Fluff Type

All functions are of [FluffType](../src/main/scala/com/github/solomonronald/spark/fluff/types/FluffType.scala) trait. So to start creating a custom function, create a new `FluffType` in [types](../src/main/scala/com/github/solomonronald/spark/fluff/types) package.

``` scala
// My new custom fluff function 
class CustomFluff(nullPercent: Int = DEFAULT_NULL_PERCENTAGE) extends FluffType with Serializable {

}
```

## Step 2: Implement custom logic

After creating a function class we need to implement/override the `FluffType` functions. Which are:

- `needsRandomIiD`
- `getColumn`
- `nullPercentage`

``` scala
// My new custom fluff function 
class CustomFluff(nullPercent: Int = DEFAULT_NULL_PERCENTAGE) extends FluffType with Serializable {
  /**
   * First: We need to tell Fluff if this custom function needs a random i.i.d.
   * 
   * Set to true if your function needs the i.i.d. provided by Spark MLlib.
   * Set it to false for functions that generate a constant value like constant() function.
   */
  override val needsRandomIid: Boolean = true
  
  /**
   * Second: Create out core logic.
   *    
   * Spark column expression to generate custom random column value.
   * @param randomIid floating point random value column for output
   * @param nullIid floating point random value column for null percentage
   * @return column with custom random value resolved.
   */
  override def getColumn(randomIid: Column, nullIid: Column): Column = {
    // Add your custom logic here...
    // Use the randomIid variable to generate your custom random logic
    // randomIid is an i.i.d column provided by Spark.
    // randomIid is a decimal value that ranges from 0 to 1 with 16 decimal precision.
    
    // Do use withNull function to handle null percentage.
    withNull(lit(const), nullIid, nullPercent)
  }
  
  /**
   * Third: Get the null percentage
   *
   * Get the null percentage value of Fluff Type column
   * @return null percentage
   */
  override def nullPercentage: Int = this.nullPercent
  
}
```

## Step 3: Add a parser

In order to parse the `functionExpr` string, we'll create a helper object that implements [FluffObjectType](../src/main/scala/com/github/solomonronald/spark/fluff/types/FluffObjectType.scala).

``` scala
object CustomFluff extends FluffObjectType {
  /**
   * This name id should be a unique string by which functions will be separated.
   * So if your function name is "custom()" then your name name id should be "custom" 
   */
  val NAME_ID: String = "custom"

  /**
   * You can also create an optional separate parse function.
   * This function will be called with raw "expr" string value.
   * The "expr" field can be used to initialize your custom function 
   * @param expr function expression
   * @return
   */
  def parse(expr: String): CustomFluff = {
    // Get percentage value for custom() by Using FunctionParser helper method
    val parsedResult = FunctionParser.parseInputParameters(expr)
    
    // Return instance of CustomFluff
    new CustomFluff(parsedResult._2)
  }
}
```

## Step 4: Add your function entry

Last step is to add your custom function `NAME_ID` to Function converter.
To do so add a new entry in the `match` block of `convertFromExpr` function in [FluffyFunction](../src/main/scala/com/github/solomonronald/spark/fluff/ops/FluffyFunction.scala) __object__.

``` scala
case CustomFluff.NAME_ID => CustomFluff.parse(expr)
```

That's it! you have created your own custom Fluff Function Type.  
Please refer existing function implementation from [types](../src/main/scala/com/github/solomonronald/spark/fluff/types) package.
