#! /usr/bin/env python3

import sys
from pyspark import SparkContext

sc = SparkContext("local", "Movie Ranking")#sys.argv[1], "Movie Ranking")
review_lines = sc.textFile(sys.argv[1])#2])
#movie_lines = sc.textFile(sys.argv[3])
reviews = review_lines.map(lambda line: line.split(","))
#movies = movie_lines.flatMap(lambda line: line.split(","))

review_freq = reviews.map(lambda (userId, movieId, rating, timestamp): (movieId, 1))
review_counts = review_freq.reduceByKey(lambda a, b: (a + b))
inverted_review_counts = review_counts.map(lambda (k, v): (v, k))
sorted_review_counts = inverted_review_counts.sortByKey(False)
problem1_output_p1 = sorted_review_counts.take(10)
#for (count, word) in problem1_output:
#	print("{} {}".format(word, count)) 
problem1_output_p1.collect()


