# Spark Fluff

_Spark Fluff_ is a distributed random data generation tool for [Apache Spark](https://spark.apache.org/). _Fluff_ can generate large amount of random data quickly and in a distributed way. Most common use case for _Fluff_ is to load test your big data applications.

## Wiki

Please visit [Spark Fluff Wiki](https://github.com/solomonronald/spark-fluff/wiki) for more details.

## Overview

At it's core _Spark Fluff_ uses [Spark MLlib's RandomRDDs](https://spark.apache.org/docs/3.0.0-preview/api/scala/org/apache/spark/mllib/random/RandomRDDs$.html) to generate random data. All you need to get started is a column definition of expected output. This column definition can be provided as a separate csv file so that you don't have to compile your code every time you want to generate different schema. _Fluff_ will then return a Spark `DataFrame` object which you can manipulate further or just write it directly to file system as a csv, parquet, etc.

## Usage

### Step 1: Add dependencies

For __Maven__ projects add this to your `pom.xml`

``` xml
<dependency>
    <groupId>com.solomonronald.spark</groupId>
    <artifactId>spark-fluff</artifactId>
    <version>1.0.1</version>
</dependency>
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

More columns details can be found [here](https://github.com/solomonronald/spark-fluff/wiki/Column-Details).

### Step 3: Generate data with the following code

``` scala
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

More examples for _Fluff_ can be found here: [Spark Fluff Examples](https://github.com/solomonronald/spark-fluff-examples)

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

More details about Fluffy Functions can be found [here](https://github.com/solomonronald/spark-fluff/wiki/Fluffy-Functions).

### Null Values

You can mention the percentage probability of the column having null values by adding `[nullPercentage%]` to the end of any function expression.

For example if you want to set 10% of rows to null for uuid column, you can do as following in the csv file:

```csv
1,UUID,string,uuid()[10%]
```

Adding a null percentage to a function is optional and can be added to any function expression. Only integer values from 0 to 100 are accepted and default is 0% if no value is mentioned explicitly.

### Separate Functions CSV

With columns csv file, you can also provide an extra functions csv file. This functions csv file can contain only your function expressions with function names. You can refer to the functions defined in functions csv file into your columns csv file using `$functionName`, so that you can reuse single function multiple times. Using a functions csv is encouraged to reduce memory usage.

#### Example Functions CSV

| functionName | functionExpr |
| :--- | :--- |
| myRange | range(0\|100\|2)[20%] |

#### Example Columns CSV referring functions from Functions CSV

|index|name|type|functionExpr|
|:---|:---|:---|:---|
|1|UUID|string|uuid()|
|2|Random_Range1|string|$myRange|
|3|Random_Range2|string|$myRange|

## Sample CSV Files

- Sample Independent Columns CSV: [columns2.csv](./src/test/resources/columns2.csv), [columns3.csv](./src/test/resources/columns3.csv)
- Sample Functions CSV: [functions1.csv](./src/test/resources/functions1.csv)
- Sample Columns CSV: [columns1.csv](./src/test/resources/columns1.csv)

## Code Examples

Example/Implementation of Spark Fluff can be found [here](https://github.com/solomonronald/spark-fluff-examples).
