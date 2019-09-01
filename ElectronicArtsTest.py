import argparse
import json
import requests
from pyspark.sql import functions as F
from datetime import datetime, timedelta
from dateutil.parser import parse

def get_spark_commits(date_str):
    # 2.1: Change the github_api_url so that it queries with the input date
    # Convert the date string into date formate
    fromDate = datetime.strptime(date_str, '%Y%m%d').date()
    toDate = fromDate + timedelta(days=1)

    # Construct the Git URL to fetch JSON Object(s)
    request = 'https://api.github.com/repos/apache/spark/commits?since=' + str(fromDate) + 'T00:00:00Z&until=' + str(toDate) + 'T00:00:00'
    print('Beginning file download from: ' + request)

    import urllib.request, urllib.error
    try:
        # Get the json object(s) with Git URL
        response = urllib.request.urlopen(request)
    except urllib.error.HTTPError as e:
        # Return code error (e.g. 404, 501, ...)
        print('HTTPError: {}'.format(e.code))
    except urllib.error.URLError as e:
        # Not an HTTP-specific error (e.g. connection refused)
        print('URLError: {}'.format(e.reason))
    else:
        # 200
        sourceJASON = response.read()

    import pandas as pd

    # response.read() returns a bytes object, which is just a sequence of bytes.
    # You need to decode it first, because Python doesn't know what the bytes represent.
    jsonData = json.loads(sourceJASON.decode('utf-8'))

    from pyspark import SparkContext
    # Create Spark Context Directly by passing the config parameters
    sc = SparkContext("local[*]", "PySpark Electronic Arts Test")

    # from pyspark import SparkSession
    from pyspark.sql import SparkSession
    spark = SparkSession(sc)
    # Create a Spark DataFrame from a Pandas DataFrame using Arrow
    # Pandas DataFrame is not distributed it exists on Driver node only
    # Inorder to acheive parallisam we need to distribute the data across the cluster
    # Spark DataFrame will distribute the DataFrame
    source_df = spark.createDataFrame(pd.DataFrame(jsonData))

    source_df.printSchema
    source_df.show()

    from pyspark.sql.types import DateType, IntegerType

    # Create a new DataFrame by selecting only few Key Value Pairs from the original JSON Object(s)
    jsonDF = source_df.select(source_df.sha.alias('sha') \
                              , source_df.author.login.alias('login_name') \
                              , source_df.committer.id.cast(IntegerType()).alias('commiter_id') \
                              , F.concat_ws(' ', F.map_values(source_df.commit.message)).alias('message') \
                              , source_df.commit.author.date.cast(DateType()).alias('commit_date') \
                              , source_df.commit.author.email.alias('email') \
                              , F.substring_index(source_df.commit.author.email, '@', -1).alias('email_company') \
                              , source_df.url.alias('url'))

    # Save this DataFrame in memory as it will be used multiple times in the future
    jsonDF.cache()
    jsonDF.printSchema
    jsonDF.show()

    # Set Parameters for PostgreSQL Database Connection
    url_connect = "jdbc:postgresql://pa1postgreserver.postgres.database.azure.com:5432/postgres?"
    commitTable = "F_SPARK_COMMITS"
    authorTable = "F_SPARK_AUTHORS"
    mode = "append"
    db_properties = {"user": "pavanpostgre@pa1postgreserver", "password": "TestPassword1",
                     "driver": "org.postgresql.Driver"}

    # Read the Authors Table from PostgreSQL DB into a Spark DataFrame Object
    readAuthorTableDF = spark.read.jdbc(url=url_connect, table=authorTable, properties=db_properties)

    # Check if the Authors table is empty or not
    # If the table in the db is empty then insert the authors dataframe directly
    # If the table is not empty join the 2 author tables and filter the existing authors in db_properties
    # Insert only the new author records into DB table
    if len(readAuthorTableDF.head(1)) > 0:
        authDF = jsonDF.join(readAuthorTableDF, jsonDF.login_name == readAuthorTableDF.login_name, how='left') \
            .filter(readAuthorTableDF.login_name.isNull()) \
            .select(jsonDF.login_name \
                    , jsonDF.commiter_id \
                    , jsonDF.email \
                    , jsonDF.email_company)
    else:
        authDF = jsonDF.select(jsonDF.login_name \
                               , jsonDF.commiter_id \
                               , jsonDF.email \
                               , jsonDF.email_company)
    authDF.write.jdbc(url=url_connect, table=authorTable, mode="append", properties=db_properties)
    authDF.show()

    # Read the Authors table after insearting the new authors
    readAuthorTableDF = spark.read.jdbc(url=url_connect, table=authorTable, properties=db_properties)
    # Read the Commits Table from PostgreSQL DB into a Spark DataFrame Object before Update
    readCommitTableDF = spark.read.jdbc(url=url_connect, table=commitTable, properties=db_properties)
    # Create DataFrame by joining the DataFrame which is createded from the source JSON with the authors table contents
    # Do a InnerJoin with authors dbtable data frame to fetch only the records that have a commit_id in authors table
    commitDF = jsonDF.join(readAuthorTableDF, jsonDF.commiter_id == readAuthorTableDF.commiter_id, how='inner')

    from pyspark.sql import Row

    # Check if the Commits table is empty or not
    # If the table in the db is empty then insert the commits dataframe directly
    # If the table is not empty then check the last executed date in the commits db table
    # Now filter all the records with the current date as last executed datetime
    # The above step will make sure the process is idempotent.
    # Insert only the new author records into Commits DB table
    if len(readCommitTableDF.head(1)) > 0:
        maxDate = readCommitTableDF.orderBy(readCommitTableDF.creation_date.desc()).head(1)[0].creation_date
        commitDF = commitDF.filter(F.current_timestamp().cast(DateType()) != maxDate).select(jsonDF.sha \
                                                                                             , jsonDF.url \
                                                                                             , jsonDF.message \
                                                                                             , jsonDF.commit_date \
                                                                                             ,
                                                                                             readAuthorTableDF.author_id \
                                                                                             ,
                                                                                             readAuthorTableDF.creation_date)
    else:
        commitDF = commitDF.select(jsonDF.sha\
                                   , jsonDF.url\
                                   , jsonDF.message
                                   , jsonDF.commit_date\
                                   , readAuthorTableDF.author_id\
                                   , readAuthorTableDF.creation_date)
    commitDF.show()
    commitDF.write.jdbc(url=url_connect, table=commitTable, mode="append", properties=db_properties)

if __name__ == '__main__':
    parser = argparse.ArgumentParser("Get the spark commits at a given date")
    parser.add_argument(
        type=str,
        help='The date to be executed in the YYYYMMDD format. (Eg. 20180120)',
        required=True)

    args = parser.parse_args()
    get_spark_commits(args.date_str)