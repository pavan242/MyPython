from __future__ import print_function
import sys

# from pyspark import SparkSession
from pyspark.sql import SparkSession

def get_keyval(row):
    # get the text from the row entry
    text = row.text

    # lower case text and split by space to get the words
    words = text.lower().split(" ")

    # for each word, send back a count of 1
    # send a list of lists
    return [[w, 1] for w in words]

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("Python Spark SQL WordCount Example") \
        .config("spark.driver.host", "localhost") \
        .getOrCreate()
    df = spark.read.csv("/home/pavan/Data/greetings.csv", header=True, sep=",");

    # for each text entry, get it into tokens and assign a count of 1
    # we need to use flat map because we are going from 1 entry to many
    mapped_rdd = df.rdd.flatMap(lambda row: get_keyval(row))

    from operator import add
    # for each identical token (i.e. key) add the counts
    # this gets the counts of each word
    counts_rdd = mapped_rdd.reduceByKey(add)

    # newDF = df.select("text")
    # print(df.collect())
    # newDF.show()

    # get the final output into a list
    word_count = counts_rdd.collect()

    # print the counts
    for e in word_count:
        print(e)