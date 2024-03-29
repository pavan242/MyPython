from __future__ import print_function
import sys

from pyspark import SparkContext, SparkConf

def g(x):
    print(x)

if __name__ == "__main__":

    # create Spark context with necessary configurations
    # Create Spark Context Directly by passing the config parameters
    # sc = SparkContext("local","PySpark Word Count Exmaple")
    # Create Spark Context though conf object
    # Create config object to run spark on a remote cluster by setting the master url
    # conf = SparkConf().setAppName("PySpark App").setMaster("spark://master:7077")
    # Create config object to run spark on a locally with 2 executors
    # conf = SparkConf().setMaster("local[2]").setAppName("My Word Count Application")
    # Create config object to run spark on a locally with max number of executors 
    conf = SparkConf().setAppName("My Word Count Application").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    # read data from text file and split each line into words
    words = sc.textFile("/home/pavan/Data/greetings.txt").flatMap(lambda line: line.split(" "))

    # count the occurrence of each word
    wordCounts = words.map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)

    # save the counts to output folder
    # wordCounts.saveAsTextFile("/home/pavan/Data/Output")

    # Print to console using print function
    # wordCounts.foreach(print)
    # Print to console using UDF
    wordCounts.foreach(g)