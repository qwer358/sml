'''
Created on Feb 9, 2017

@author: vlad
'''
from pyspark import SparkConf, SparkContext
import os

if __name__ == '__main__':
    pass



# Configure the Spark environment
sparkConf = SparkConf().setAppName("WordCounts").setMaster("local")
sc = SparkContext(conf = sparkConf)

 # The WordCounts Spark program
textFile = sc.textFile(os.environ["SPARK_HOME"] + "/README.md")
wordCounts = textFile.flatMap(lambda line: line.split()).map(lambda word: (word, 1)).reduceByKey(lambda a, b: a+b)
for wc in wordCounts.collect(): print wc