# Spark Fluff

A distributed random data generation tool for [Apache Spark](https://spark.apache.org/). _Spark Fluff_ can generate large amount of random data quickly and in a distributed way.

## Overview

At it's core _Spark Fluff_ uses [Spark MLlib's RandomRDDs](https://spark.apache.org/docs/3.0.0-preview/api/scala/org/apache/spark/mllib/random/RandomRDDs$.html) to generate random data. All you need to get started is a column definition of expected output. Column definition can be provided as a separate csv file so that you don't have to compile your code every time you want to generate different schema. _Fluff_ returns a Spark `DataFrame` object which you can manipulate further or just write it directly to file system as a csv, parquet, etc.

## Usage

### Step 1: Add dependencies

The artifact is available on [Maven Central](https://mvnrepository.com/artifact/com.github.solomonronald/spark-fluff) and can be used in your build tools.

#### Maven Dependencies

For __Maven__ projects add this to your `pom.xml`

``` xml
<dependency>
    <groupId>com.github.solomonronald</groupId>
    <artifactId>spark-fluff</artifactId>
    <version>1.0.0</version>
</dependency>
```

#### SBT Dependencies

For __SBT__ projects add this to your `build.sbt`

``` scala
libraryDependencies += "com.github.solomonronald" % "spark-fluff" % "1.0.0"
```

### Step 2: Create a columns schema csv file

Create a csv file with the following content: ([Or use this csv file](./src/test/resources/columns3.csv))

|index|name|type|functionExpr|
|:---|:---|:---|:---|
|1|UUID|string|uuid()|
|2|Random_Val|double|range(0\|1\|6)|
|3|Some_Constant|string|const(k)|
|4|Random_Vowel|string|list(a\|e\|i\|o\|u)|
|5|Random_Date|string|date(2000-01-01 00:00 \| 2030-12-31 23:59 \| yyyy-MM-dd HH:mm)
|6|Random_Bool|boolean|bool()

### Step 3: Generate data with the following code

``` scala
// Import Fluff
import com.github.solomonronald.spark.fluff.Fluff

// ... get/create a Spark Session in sparkSession ...

// Your input columns CSV File path
val yourInputCsvFilePath: String = "<your path to>/<file name>.csv"

// Create fluffy DataFrame with data defined in csv files
val fluffyDf: DataFrame = Fluff(sparkSession).generate(yourInputCsvFilePath, numRows = 100)
    
// Show a sample
fluffyDf.show(5)
```

And that's it! The above code will generate following random data:

|UUID|Random_Val|Some_Constant|Random_Vowel|Random_Date|Random_Bool
|:---|:---|:---|:---|:---|:---|
|85881d64-8bfe-490e-8ec2-83253d834f39|0.593161|k|u|2006-10-02 18:28|false|
|6234b5a0-7c80-413c-87cc-69e71c10fca2|0.774724|k|u|2029-04-21 11:48|true|
|31b1104d-4717-4d55-90a9-556bdffbacb5|0.40595|k|a|2006-10-22 18:49|true|
|2456e2cf-051e-455e-be9b-1de024be2439|0.915863|k|o|2023-11-07 14:03|false|
|b5ba5820-f74c-496e-8451-e37ac5d0395c|0.597763|k|i|2007-05-02 21:03|true|

## Functions

Following functions are available to generate data using _Fluff_

| Function | Description |
| :-- | :-- |
| uuid() | Generates a random UUID. |
| range(min\|max\|precision) | Picks a value from range [min, max) with specific precision. |
| list(value1\|value2\|...) | Picks a value from a list of "\|" delimited items. |
| date(start\|end\|format) | Picks a date from range [start, end) in specified format. |
| const(value) | Generates a constant value for all rows. |
| bool() | Generates `true` or `false`. |

More details about Fluff Functions can be found [here](./docs/fluff-functions.md).

You can also create your own custom functions by following these [instructions](./docs/create-function.md).

## Null Values

You can mention the percentage probability of the column having null values by adding `[nullPercentage%]` to the end of any function expression.

For example if you want to set 10% of rows to null for uuid column, you can do as following in the csv file:

```csv
1,UUID,string,uuid()[10%]
```

Adding a null percentage to a function is optional and can be added to any function expression. Only integer values from 0 to 100 are accepted and default is 0% if no value is mentioned explicitly.

__Note:__ The null percentage is actually the probability of that record to have null value.

## Separate Function Definition

You can also provide an extra `functions.csv` file (containing your function definitions) along with usual `columns.csv` file (containing your column definition).  
This `functions.csv` file must contain function expressions with function names __only__. The functions defined in `functions.csv` can be now referred in `columns.csv` file using `$functionName`, so that a single function can be reused multiple times.

__Note:__ Using a `functions.csv` is __highly recommended__ in order to reduce memory pressure on your executors.

### Example for using a separate `functions.csv`

#### Step 1: Create a csv file with following function definition

Example `functions.csv`

| functionName | functionExpr |
| :--- | :--- |
| myRange | range(0\|100\|2)[20%] |

#### Step 2: Refer the functions defined in the above file in your columns definition csv file

Example `columns.csv` referring functions from `functions.csv`

|index|name|type|functionExpr|
|:---|:---|:---|:---|
|1|UUID|string|uuid()|
|2|Random_Range1|string|$myRange|
|3|Random_Range2|string|$myRange|

#### Step 3: Add the function definition file to your code

``` scala
// Import Fluff
import com.github.solomonronald.spark.fluff.Fluff

// ... get/create a Spark Session in sparkSession ...

// Your input columns CSV File path
val inputColumnsCsvFilePath: String = "<your path to>/columns.csv"

// Your input functions CSV file path
val inputFunctionsCsvFilePath: String = "<your path to>/functions.csv"

// Create fluffy DataFrame with data defined in csv files
val fluffyDf: DataFrame = Fluff(spark).generate(
    // Set columns csv path
    columnsCsvPath = inputColumnsCsvFilePath,
    // Set functions csv path
    functionsCsvPath = inputFunctionsCsvFilePath,
    // Set number of rows to be generated
    numRows = 100
)
    
// Show a sample
fluffyDf.show(5)
```

## Spark Fluff Examples

Examples for _Spark Fluff_ can be found [here](https://github.com/solomonronald/spark-fluff-examples).

## Sample CSV Files

- Sample Independent Columns CSV: [columns2.csv](./src/test/resources/columns2.csv), [columns3.csv](./src/test/resources/columns3.csv)
- Sample Functions CSV: [functions1.csv](./src/test/resources/functions1.csv)
- Sample Columns CSV (dependent on function csv): [columns1.csv](./src/test/resources/columns1.csv)
