#!usr/bin/env python3
import sys
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import types

#Author: Shiva Govindaraju
#Spring COEN 242
#Assignment 3 - Exploring Spark Ecosystem (Part 1)

# SOLUTION TO PROBLEM 1

#First, Create a SparkSession
spark = SparkSession.builder.master(sys.argv[1]). \
        appName("Movie Ranking Soln1 Testing").getOrCreate()
spark.sparkContext.setLogLevel("WARN") # COMMENT THIS OUT FOR FULL LOG DETAILS

#Grab the reviews data out of the csv. 
#Convert the ratings into Doubles (as they are normally kept as Strings).
#Cache the Movie-Reviews data for good measure, to keep it ready for later use.
reviews_dat_raw = spark.read.format('csv').options(header=True, \
        ignoreLeadingWhiteSpace=True, ignoreTrailingWhiteSpace=True). \
        load(sys.argv[2])
mr_dat = reviews_dat_raw.withColumn("rating", \
        reviews_dat_raw['rating'].cast(types.DoubleType()))
'''mr_dat.cache()'''

#Group the Movie-Reviews by their movieID and create a 'count' column
#based on the counts of movieId.
#Cache it as we'll reuse it later.
mr_dat_counted = mr_dat.groupBy('movieId').count()
'''mr_dat_counted.cache()'''

#Sort the Movie-Reviews DataFrame by counts and take the top 10.
prob1_output = mr_dat_counted.orderBy('count', ascending=False).limit(10)

#grab the movies data out of the csv
movies_dat = spark.read.format('csv').options(header=True, \
        ignoreLeadingWhiteSpace=True, ignoreTrailingWhiteSpace=True). \
        load(sys.argv[3])

#Problem 1 Ouput
mr_dat_final_1 = prob1_output.join(movies_dat, 'movieId', 'outer').na.drop(). \
        orderBy('count', ascending=False)
mr_dat_final_1.show()
mr_dat_final_1.coalesce(1).toPandas().to_csv("output_prob1.csv")


