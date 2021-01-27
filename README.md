# Spark Fluff

_Spark Fluff_ is a distributed random data generation tool for [Apache Spark](https://spark.apache.org/). _Fluff_ can generate large amount of random data quickly and in a distributed way which can then be used to load test your applications.

## Overview

At it's core _Spark Fluff_ requires two inputs. A column definition and a function definition in a csv format. _Fluff_ will then return a Spark `DataFrame` object which you can further manipulate or just write it directly to disk as a csv, parquet, etc.

## Usage

Add this to your `pom.xml`
``` xml
<dependency>
  <groupId>com.solomonronald.spark</groupId>
  <artifactId>spark-fluff</artifactId>
  <version>1.0.0</version>
</dependency>
```

And use the following code

``` scala
// Create fluffy data frame with data defined in csv files
val fluffyDf: DataFrame = Fluff(spark).generate(
    columnsCsvPath = inputColumnsCsvPath, functionsCsvPath = inputFunctionsCsvPath, numRows = 100)
    
// Show a sample
fluffyDf.show(5)
```

## Fluff Column

_Fluff_ requires a schema on the basis of which data is generated. This can be provided as a csv file or you can manually create an array of [FluffyColumn](./src/main/scala/com/solomonronald/spark/fluff/ops/FluffyColumn.scala) in your Spark - Scala code.

### Schema of Columns CSV file

| Column Name | Description |
| --- | --- |
| Index | Index by which columns will be ordered in output |
| Column Name | Name of output column |
| Column Data Type (Cast As) | Cast output column to this data type |
| Function | Function name or function expression to be applied on column |

The fluff column schema consists of 4 items.

1. __Index__ to maintain ordering of output columns. As the input will be distributed, so in order to preserve ordering of output column an index is required.

1. __Column Name__ will be the name of output column in data frame. You can query on data frame returned by _Fluff_ using this column name.

1. __Column Data Type__ is the data type of the output column. The output column will be explicitly casted as the data type mentioned under this value. Valid/Supported values are strings which are acceptable by [org.apache.spark.sql.Column](https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/Column.html#cast-java.lang.String-) namely: `string`, `boolean`, `byte`, `short`, `int`, `long`, `float`, `double`, `decimal`, `date`, `timestamp`.

1. __Function__ is the function name or function expression. If you want functions declared in a separate file you can create a functions.csv file from which function names can be referred here. Functions can be referred with `$` symbol. Ex. A function `func1` declared in functions.csv file can be referred as `$func1` here. Or you can directly write function expression in this column ex. `uuid()`.

#### Example for Columns CSV

| Index | Column Name | Cast As | Function |
| --- | --- | --- | --- |
| 1 | col1 | string | $func1
| 2 | col2 | string | $func2
| 3 | col3 | string | uuid()
| 4 | col4 | int | range(0, 2, 0)
| 5 | col5 | date | $func3
| 2 | col6 | string | $func2

__NOTE:__ Input columns csv file does not require a header

### Fluff Functions

_Fluff_ provides an option to give a separate input for functions. The functions input is a csv or manual declaration of an array of function names and function expressions ([FluffyFunction](./src/main/scala/com/solomonronald/spark/fluff/ops/FluffyFunction.scala)). __These functions can be referred in columns.csv input file using `$`__. The motive to keep a separate functions file is to enable reuse of common functions in columns.csv file. Internally this function file is _broadcasted_ over all nodes as a map, in order to avoid repeated calls.

#### Schema for Fluff Functions CSV file

| Column Name | Description |
| --- | --- |
| Function Name | Name of the function which can be used in columns csv |
| Function Expression | Expression for function |

__NOTE__ Function Name in functions.csv should not start with an underscore `_` or a dollar sign `$`.

#### Example for Fluff Functions CSV file

| Function Name | Function Expression |
| --- | --- |
| func1 | uuid() |
| func2 | range(a,b,c) |
| func3 | date(2000-01-01 00:00:00, 2030-12-31 23:59:59, yyyy-MM-dd HH:mm:ss) |

__NOTE:__ Input functions csv file does not require a header

#### Fluff Function Types

Fluff provides the following functions which can be used to generate random data

1. [Array Type](./src/main/scala/com/solomonronald/spark/fluff/types/ArrayFluff.scala)

    ##### Format
    ```
    array(<comma separated values>)
    ```

    ##### Example
    ```
    array(apple,bat,cat,dog)
    ```

    ##### Description

    An array of possible values is passed to _Fluff_ and one of them will be picked at random and added to the row.

1. [Constant Type](./src/main/scala/com/solomonronald/spark/fluff/types/ConstFluff.scala)

    ##### Format
    ```
    const(<constant string>)
    ```

    ##### Example
    ```
    const(hello)
    ```

    ##### Description

    A constant value will be applied to all rows.

1. [Date Type](./src/main/scala/com/solomonronald/spark/fluff/types/DateFluff.scala)

    ##### Format
    ```
    date(<start date>, <end date>, <date format>)
    ```

    ##### Example
    ```
    date(2000-01-01 00:00:00, 2030-12-31 23:59:59, yyyy-MM-dd HH:mm:ss)
    ```

    ##### Description

    A date is picked in range between start date and end date as the first and second parameters respectively. The third parameter is the date format (any valid [SimpleDateFormat](https://docs.oracle.com/javase/7/docs/api/java/text/SimpleDateFormat.html) string).

1. [Range Type](./src/main/scala/com/solomonronald/spark/fluff/types/RangeFluff.scala)

    ##### Format
    ```
    range(<min value>, <max value>, <decimal precision>)
    ```

    ##### Example
    ```
    range(0, 1, 6)
    ```

    ##### Description

    A value is picked at random between min and max value as the first and second parameters respectively. The third optional parameter is the decimal precision you want in the output.
    
    __NOTE__ that end range is exclusive [minValue, maxValue). To generate an integer in the range of 1 to 10 you'll have to create range as `range(1, 11, 0)`.

1. [UUID Type](./src/main/scala/com/solomonronald/spark/fluff/types/UuidFluff.scala)

    ##### Format
    ```
    uuid()
    ```

    ##### Example
    ```
    uuid()
    ```

    ##### Description

    A random UUID value is generated for each row.
    
    __NOTE__ that UUID is not dependent on `seed` value.

## Input CSV file structure

- The input csv files (columns.csv or functions.csv) are pipe `|` delimited .csv files.
- Header should not be present in these files.

### Example Files

- Sample Functions CSV: [functions1.csv](./src/test/resources/functions1.csv)
- Sample Columns CSV: [columns1.csv](./src/test/resources/columns1.csv)
- Sample Independent Columns CSV: [columns2.csv](./src/test/resources/columns2.csv)

### Code Examples

Example/Implementation of Spark Fluff can be found [here](https://github.com/solomonronald/spark-fluff-examples).
