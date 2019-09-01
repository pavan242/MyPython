from __future__ import print_function
import sys

# from pyspark import SparkConf
from pyspark import SparkContext

# from pyspark import SparkSQLContext
from pyspark.sql import SQLContext

if __name__ == "__main__":

    # Create Spark Context Directly by passing the config parameters
    sc = SparkContext("local[*]","PySpark Word Count Exmaple")

    sqlContext = SQLContext(sc)
    df = sqlContext.read\
        .option("multiLine", True)\
        .option("mode", "PERMISSIVE")\
        .json("/home/pavan/Data/greetings.json")

    # Remove all the rows with any of its column having a null value
    df1 = df.na.drop(how='any')
    df1.show(3)
    df1.describe().show()

    # Remove the rows with text column having empty string
    df.filter(df.text != "").show()
    # This is to display only the value of text column
    df.select("text").show()

    # This creates a view of the json dataset
    df.createOrReplaceTempView("json_view")

    # issue the SQL query to select only the 'text' field
    dfNew = sqlContext.sql("select text from json_view")
    # issue the SQL query to select only the 'title' field
    dfNew = sqlContext.sql("select title from json_view")
    dfNew.show()