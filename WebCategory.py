import json
# import pandas as pd
import requests
# import urllib.request

def getResponseFromUrl(url):
    headers = {'Content-Type': 'application/json'}
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        return response.content.decode('utf-8')
    else:
        return None

if __name__ == '__main__':

    # Download a json file from web service
    #url = "http://asos-prod-gbl-pdt-category-api-tm.trafficmanager.net/categories/v1/categories?store=1&lang=en-gb"
    #print('Beginning file download from: ' + url)

    # Set the Path for copying the output file
    # path = "/dbfs/mnt/csv/webcategory/"
    # path = "/home/pavan/Data/webcategory/"
    path = "/home/pavan/Data/"

    # Replace or create the directory in Databricks
    # dbutils.fs.mkdirs(path.replace("/dbfs", "dbfs:"))

    # Set the file name along with the path
    file = path + "webcategory.json"

    # Download the file from web URL using request function
    #urllib.request.urlretrieve(url, file)
    # Alternatively you can also get it using user defined function
    # response = getResponseFromUrl(url)

    # Opening a file and reading it in Python
    with open(file) as json_file:
        # Reads JSON from file. The result will be a Python dictionary.
        data = json.load(json_file)
        # load() loads/deserialize JSON from a file or file-like object
        # loads() loads/deserialize JSON from a given string or unicode object
        # https://stackoverflow.com/questions/39719689/what-is-the-difference-between-json-load-and-json-loads-functions

    # Returns type of the given object.
    type(data)
    responseJson = data

    # Gets the values for Categories in the Python object and converts it into JSON object
    categoriesJson = responseJson['categories']

    # If you want to Dump the JSON into a file/socket or whatever, then you should go for dump().
    # If you only need it as a string (for printing, parsing or whatever) then use dumps()
    categories_string = json.dumps(categoriesJson)

    from pyspark import SparkContext, SparkConf
    conf = SparkConf()\
        .setAppName("My Web Category Application")\
        .setMaster('local[*]')
    sc = SparkContext(conf=conf)

    # Split the string into partitions and assign to executors
    rdd = sc.parallelize([categories_string])

    #from pyspark.sql import SparkSession
    #spark = SparkSession.builder.config(conf=conf)
    from pyspark.sql import SQLContext
    sqlContext = SQLContext(sc)
    # Convert the rdd of Categories into a Data Frame
    df = sqlContext.read.json(rdd)

    # This will print the Schema of the Categories Object
    df.printSchema()

    # Displays the contents of the Categories Object
    df.show()
    # from pyspark.sql.functions import split, explode
    from pyspark.sql.functions import *
    df.select(explode('categories')).show()

    from pyspark.sql import Row, Column
    # Select few columns in the JSON file
    dfRoot = df.select(Column('id').alias('parentId'), Column('id').alias('childId'), 'friendlyName', 'categories')

    # Display the results of the selected Columns
    dfRoot.show()

    from pyspark.sql.functions import *

    # Recursive function to assign the Parent and Child Ids appropritely
    def flattenProductHierarchyRecursive(df):
        # "explode" function creates a new row for each element in the given array or map column (in a DataFrame).
        if df.select(explode('categories')).count() <= 0:
            return df.select('parentId', 'childId', 'friendlyName')
        else:
            dfR = df.select('childId',explode('categories').alias('CatArray'))\
                .select(Column('childId').alias('parentId'), Column('CatArray.id').alias('childId'), Column('CatArray.friendlyName').alias('friendlyName'), Column('CatArray.categories').alias('categories'))
        return df.select('parentId', 'childId', 'friendlyName')\
            .union(flattenProductHierarchyRecursive(dfR).select('parentId', 'childId', 'friendlyName'))

    ph = flattenProductHierarchyRecursive(dfRoot)
    ph.show()

    #df = ph.toPandas().rename(columns={'parentId': 'ParentId', 'childId': 'WebCategoryId', 'friendlyName': 'Name'})
    #df = df[df.Name.notnull()]
    #df.show()

    # Set the output file path along with its name
    #output_file = path + "webcategory.csv"
    #df.to_csv(output_file, index=False, header=True)