#!usr/bin/env python3
import sys
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import types

#Author: Shiva Govindaraju
#Spring COEN 242
#Assignment 3 - Exploring Spark Ecosystem (Part 1)

# SOLUTION TO PROBLEM 2

#First, Create a SparkSession
spark = SparkSession.builder.master(sys.argv[1]). \
        appName("Movie Ranking Soln2 Testing").getOrCreate()
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

#Group the joined DFs by their movieID
#Create an 'avg' column based on each movie's ratings
mr_dat_reviewed = mr_dat.groupBy('movieId').avg('rating')

#Join the counts to the averages (based on movieId) for a unified DF
mr_dat_ratedcounts = mr_dat_reviewed.join(mr_dat_counted, 'movieId', 'outer')

#Filter the DF for counts > 10 and ratings > 4.0 then output them all
#Sort the output by avg(rating)) for ease of perusal
prob2_output = mr_dat_ratedcounts.filter(mr_dat_ratedcounts['count'] > 10). \
        filter(mr_dat_ratedcounts['avg(rating)'] > 4.0). \
        orderBy('avg(rating)', ascending=False)

# Both Problem 1 and Problem 2 are solved. Now we just need to output them

#grab the movies data out of the csv
movies_dat = spark.read.format('csv').options(header=True, \
        ignoreLeadingWhiteSpace=True, ignoreTrailingWhiteSpace=True). \
        load(sys.argv[3])

#Problem 2 Output (only shows top 100 to terminal for ease of display)
mr_dat_final_2 = prob2_output.join(movies_dat, 'movieId', 'outer').na.drop(). \
        orderBy('avg(rating)', ascending=False)
mr_dat_final_2.show(n=100)
mr_dat_final_2.coalesce(1).toPandas().to_csv("output_prob2.csv")

