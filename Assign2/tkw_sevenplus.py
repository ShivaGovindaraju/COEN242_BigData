#!/usr/bin/env python
import sys
from collections import Counter
#Author: Shiva Govindaraju

''' Parse input into Counter if the word is > 6 characters '''
cntr = Counter()
for line in sys.stdin:
	key, value = line.strip().split("\t", 2)
	word = key.replace('"','')
	if len(word) > 6:
		cntr[word] += int(value)
''' Output the top 100 counts of the Counter to stdout '''
for word, count in cntr.most_common(100):
	print("{}\t{}".format(word, count))
