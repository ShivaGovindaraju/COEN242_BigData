import numpy as np
from collections import Counter
import re
from time import time, sleep
import itertools
import psutil
import os

# Author: Shiva Keshav Govindaraju
# Class: COEN 242 Big Data
# Assignment 1 Top K Words

# Trying out a quick and dirty implementation first

def countWords(filename):
    """ Counts the Words in a File
    Args:
        filename: a string containing the filename with text to be read and counted
    Returns:
        Counter object with unique words as keys and word-counts as values
    """
    with open(filename) as f:
        filetext = f.read()
    #words = re.findall(r'\w+', filetext)
    words = re.findall(r'[a-zA-Z]+', filetext)
    return Counter(words)

def kMostFrequentWords(filename, k):
    """K More Frequent Words in a File
    Args:
        filename: a string containing the filename with text to be read and counted
    Returns:
        List of key-value pairs in tuples in the format ("word", word_count)
        where "word" is a string and word_count is an integer
    """
    wordcounts = countWords(filename)
    return wordcounts.most_common(k)

# This takes *forever* on large files
# And the python open() function is unsufficient for large files as is file.read()

# I shall resolve this issue by "sharding" the files then processing the Shards

# First, I'll make some helper functions to make things a little easier...

def findFLength(filename):
    """Finds the Length of a File in Lines
    Args:
        filename: a string contianing the filename
    Returns:
        Integer with the number of lines in the file given by filename
    """
    f = os.popen('wc -l < {}'.format(filename))
    return int(f.read())

def splitFileIntoShards(filename, shardsize):
    """Splits Input File Into Shards
    Args:
        filename: a string containig the filename of the file to be sharded
        shardsize: the number of lines each file should have
    """
    os.popen('split -a 4 -d --additional-suffix=_shard -l{} {}'.format(shardsize, filename))

def deleteShards():
    """Deletes All Shard Files with Filenames ending in '_shard'
    """
    os.popen('rm *_shard')

def deleteSingleFile(filename):
    """Deletes a single File
    Args:
        filename: a string continaing the filename of the file to be deleted
    """
    os.popen('rm {}'.format(filename))

# This is the main function that runs the overall algorithm for my design.    

def findTopKViaSharding(filename, k, shards):
    """Finds the Top K Words in a File
    
    This function takes in a file and splits it into a given number of shards.
    It then processes each individual shard to determine the count of each unique word 
    in that shard and then compiles all those word-counts together. 
    Once all shards have been processes, the function returns the top K 
    most-frequent/common words in the entirety of the input file.
    
    Args:
        filename: a string containing the filename of the input dataset
        k: the number of most-common words to be returned
        shards: the number of shards the function should split the input file into for processing.
    Returns:
        List of key-value pairs in tuples in the format ("word", word_count) 
        where "word" is a string and word_count is an integer. 
        This List contains the Top K most-frequent/common words in the input dataset.
    Raises:
        OSError: An error occurs when trying to work with a shard-file.
    """
    
    # First, we calculate the number of lines for each shard
    filelength = findFLength(filename)
    shardsize = int(np.ceil(filelength / shards))
    
    # Now we split the input file into shards
    splitFileIntoShards(filename, shardsize)
    sleep(1) #might change this to sleep(shards/100) if 1 second is too little
    
    # Some Initial Set-Up...
    filenames = ["x{:04d}_shard".format(i) for i in range(shards)]
    totaldict = Counter()
    #shardtimes = []
    
    # Iterate through the Shard Files
    # In each Shard, run countWords(shardfilename) to get a wordcount Counter
    # Update the overall Counter in totaldict with more wordcounts from the Shard
    # Delete the Shard File when we're done with it.
    for f in filenames:
        #print("Running Shard {}...".format(f), end=' ')
        #startf = time()
        
        try:
            wordcounts = countWords(f)
            totaldict.update(wordcounts)
            deleteSingleFile(f)
            #print("Shard {} Complete.".format(f))
        except OSError as e:
            print("Something unexpected happened in the OS. Error: {}. Moving on...".format(e))
        
        #endf = time()
        #shardtimes.append(endf - startf)
    
    #print("\nDone Sharding")
    #deleteShards()
    #print("Average Shard Processing Time for K={} and Shards={} is: {}".format(k, shards, np.mean(shardtimes)))
    #print("Average Shard Processing Time for K={} and Shards={} is: {}".format(k, shards, np.std(shardtimes)))
    
    # All Shard Processing is complete. Return the Top K words from the overall Counter
    return totaldict.most_common(k)


# The following functions are all different types of Testing functions. They aren't the core of the algorithm, design and implementation.

def confirmSharding(filename, shards):
    """Test to Confirm Sharding Functionality
    
    This function tests whether Shards are created and processed properly 
    by checking whether the word-count for the sum of all shards is the same 
    as the known, overall word-count of the original dataset. 
    
    Args:
        filename: a string containing the filename of the input dataset
        shards: the number of shards the function should split the input file into for processing.
    Returns:
        Boolean saying whether the word-count of the original dataset matches 
        that of the summation of shard-file word-counts.
    Raises:
        OSError: An error occurs when trying to work with a shard-file.
    """
    #shardfilenames = splitFileIntoShards(filename, shardsize)[:-1]
    print('Start confirmation')
    filelength = findFLength(filename)
    shardsize = int(np.ceil(filelength / shards))
    splitFileIntoShards(filename, shardsize)
    #fd = os.popen('wc -l < {}'.format(filename))
    totalwordcount = filelength #int(fd.read())
    runcount = 0
    cnt = 0
    print("Starting Shard counting")
    shardfilenames = ["x{:04d}_shard".format(i) for i in range(shards)]
    for sf in shardfilenames:
        try:
            fd = os.popen('wc -l < {}'.format(sf))
            count = fd.read()
            print("current: {}".format(count))
            runcount = runcount + int(count)
        except OSError:
            print("Something unexpected happened in the OS. Moving on...")
        finally:
            cnt += 1
            print("current run cnt {}: {}".format(cnt, runcount))
    print("TotalWordCount = {}".format(totalwordcount))
    print("RunCount = {}".format(runcount))
    return totalwordcount==runcount

def runGenericTests():
    """Run the Generic Tests
    
    Runs through a batch of pre-made tests for the findTopKViaSharding() function
    to test its functionality as well as generate further output data for analytics.
    
    Mainly works on the 400MB file given by the TA, Surya: dataset-400MB.txt
    
    Returns:
        Tuple of Lists. The first List contains the shard-counts used in the tests.
        The second List in the tuple contains the overall time it takes each test
        to complete execution.
    """
    possibleK = [10, 15, 25]
    #possibleK = [15]
    #smalldata = [1, 5, 10]
    mediumdata = [10, 25, 50, 75, 100, 200, 300, 400, 500]
    #largedata = [100, 500, 1000, 5000]
    print("Running Generic Tests...")
    resulttimes = {}
    for k in possibleK:
        print("Testing Top K Words algorithm on 400MB file.")
        testtimes = []
        for test in mediumdata:
            print("Starting Test on 400MB file: Finding Top {} Words with {} Shards...".format(k, test))
            start = time()
            findTopKViaSharding("dataset-400MB.txt", k, test)
            end = time()
            print("Test Complete.")
            print("Total Time: {}".format(end - start))
            testtimes.append(end-start)
        resulttimes[k] = testtimes
    print("Generic Tests Complete.")
    return mediumdata, resulttimes
        

def MainUserInterface():
    """The Main User Interface
    
    The UI by which the user can give input to the Python script and tell the application
    which files to operate on, what values of K are needed for output, and how many shards
    the user wants to break the file into for ease of processing.
    
    It also gives the option for running the generic batch of tests in runGenericTests().
    """
    print("---Shiva Govindaraju COEN 242 Assignment 1 --- Top K Words---")

    runOption = int(input("Would you like to run the Generic Test or a Specific Test? (0 - Generic; 1 - Specific): "))
    
    if runOption == 0:
        print(runGenericTests())

    elif runOption == 1:
        print("Running Specific Test.")
        
        # Request for Input
        filename = input("Which file would you like to run on?: ")
        k = int(input("Set the number of words to output (k): "))
        shards = int(input("How many shards should be used? (Recommended [1, 5000]): "))
        
        # Ensure Shard Count is within exepected values
        while (shards > 5000 or shards < 1):
            nsh = input("That shard-count is higher than recommended. Please input shard-count [1,5000]: ")
            shards = int(nsh)
        
        #print("Current Total Memory: {} MB".format(psutil.virtual_memory()[0] / 2.**20))
        #print("Current Available Memory: {} MB".format(psutil.virtual_memory()[1] / 2.**20))
        #print("Current CPU Resource Percentage: {}".format(psutil.cpu_percent(interval=None, percpu=True)))
        print("Testing TopKWords w/ Shards on {} using {} shards.".format(filename, shards))
        print("Starting Test...")
        
        # Execute the program
        
        start = time()
        print(findTopKViaSharding(filename, k, shards))
        end = time()
        print("Test Complete.")
        
        print("Total Time: {}".format(end - start))
        #print("Total CPU Usage Percentage: {}".format(psutil.cpu_percent(interval=None, percpu=True)))
    
    elif runOption == 2:
        # This option is for testing whether Sharding worked correctly or not
        # It is not part of the code that is needed for submission, nor will it run without files I have not included in the submission
        print("Sharding Test -- Not for general use")
        print("Confirm Sharding on Ulysses: {}".format(confirmSharding("ulysses.txt", 100)))
        deleteShards()
        #print("Confirm Sharding on Indonesia: {}".format(confirmSharding("data_1gb.csv", 5000)))
        print("Confirm Sharding on Lorum: {}".format(confirmSharding("simpleTest.txt",10)))
        deleteShards()
    
    else:
        # Basic functionality tests from before I cleaned up the UI for giving the program inputs.
        
        print("OG Testing of basic functionality")
        print("Testing on Ulysses text")
        #print(findFLength("4300-0.txt"))
        start = time()
        print(kMostFrequentWords("ulysses.txt", 15))
        end = time()
        print("Total Time: {}".format(end - start))

        print("Testing on Ulysses with Shards")
        print("Top 15 Words -- 10 Shards")
        start = time()
        print(findTopKViaSharding("ulysses.txt", 15, 10))
        end = time()
        print("Total Time: {}".format(end - start))

        print("Testing TopKWords  w/ Shards on Surya's 400MB")
        print("Top 15 Words -- 1024 Shards")
        start = time()
        print(findTopKViaSharding("dataset-400MB.txt", 15, 1024))
        end = time()
        print("Total Time: {}".format(end - start))

        
#Call The Main User Interface when this script is run to start the application.        
if __name__ == "__main__":
    MainUserInterface()
