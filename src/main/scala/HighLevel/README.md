## High Level
This package contains basic information and code examples about High Level Spark API. The source codes of the following contents are included in this project.
### Data Types
___
>#### Structured
>* Includes column names and data types for each column.
>* Like a Relational Database Table.

>#### Half Structured
>* Stores records for each line and these lines seperated by a significant symbol.
>* Doesn't store data types.
>* Like .csv files.

>#### Unstructured
>* Doesn't include any schema or data type.
>* Only include data like a text or media file.


### Spark Structured API
___
Spark Structured API consists of 3 different structures. These are **DataFrame, Dataset, SparkSQL**.

>#### Spark DataFrame
>* DataFrame is the most widely used structured API.
>* The biggest difference between RDDs is the dataframes have a schema (column names and their data types).
>* DataFrame is UntypedAPI so Spark controls types in runtime.
>* Spark DataFrame has similarly concept with Pandas DataFrame.
>* Creating Spark DataFrame from Pandas DataFrame is easy.

>#### Spark Dataset
>* It is Type-Safe so its special to Scala and Java. Its not include in R and Python because they work with dynamic typed. 
>* User can specify data type with case class. 
>* Spark controls types during compile so we are less likely to encounter errors during runtime.

>#### Spark SQL Tables & Views
>* We can manipulate DataFrames and Datasets with SQL queries.
>* Operations with SQL query work with DataFrame and Dataset API. For this reason, the same code is running in the background.
>* Compatible with ANSI SQL:2003
>* It is good alternative for Hive MapReduce and Tez.
>* It is not suitable for low time queries, although suitable for big data in acceptable time.
>* It using for OLAP (Online analytical processing) not OLTP (Online transaction processing).

### Schema

---

> * It is a data structure which includes column names and column data types.
> * Structured API includes schema but unstructured not.
> * User can define schema oneself.
>> schema = StructType([\
StructField("no",IntegerType(),True),\
StructField("name",StringType(),True),\
StructField("job",StringType(),True),\
])\
df = spark.CreateDataFrame(rdd,schema)
>* Spark can create the schema on its own while reading data.
> > df = spark.read.format("csv").**option("inferSchema","true")**.load("path/file.csv")