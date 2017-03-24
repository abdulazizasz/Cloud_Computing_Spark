#HelloWorld_big_data.py

"""Calculates the word count of the given file.

the file can be local or if you setup cluster.

It can be hdfs file path"""

## Imports

from pyspark import SparkConf, SparkContext

import sys
## Constants
APP_NAME = "Minin Project 2"

def main(sc,filename):
   data = sc.textFile(filename)
   data = data.map(lambda line : line.split('\t'))
   my_data = data.map(lambda s: (s[1], s[2]))
   header = my_data.first()
   my_data = my_data.filter(lambda line : line != header)
   my_data = my_data.map(lambda s : (s[0], int(s[1])))
   result = my_data.mapValues(lambda s : (s, 1)).reduceByKey(lambda x,y : (x[0] + y[0], x[1] + y[1])).sortBy(lambda s : -s[1][0]).take(10)

   for item in result:
      print item

if __name__ == "__main__":

   # Configure Spark
   conf = SparkConf().setAppName(APP_NAME)
   # conf = conf.setMaster("local[*]")
   sc   = SparkContext(conf=conf)
   filename = sys.argv[1]
   main(sc, filename)