#!/usr/bin/env python
import sys
from collections import Counter
#Author: Shiva Govindaraju

''' Parse the data into a Counter '''
cntr = Counter()
for line in sys.stdin:
	key, value = line.strip().split("\t", 2)
	cntr[key.replace('"','')] += int(value)
''' Output the top 100 entries in the counter to stdout '''
for word, count in cntr.most_common(100):
	print("{}\t{}".format(word, count))

