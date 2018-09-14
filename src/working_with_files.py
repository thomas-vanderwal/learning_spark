# File for working with examples from Chapter 5
#  Loading and Saving Your Data
import json
import csv
import io
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("My App")
sc = SparkContext(conf = conf)

#WholeTextFile can be used to load an entire file at once
#It returns a key-value pair where the key is the name of the text file
#  - Tip: Spark can read an entire directory with * ie part-*.txt
wf = sc.wholeTextFiles('../data/asteroid_data.csv')

# Saving text files: RDD.saveAsTextFile(FILE)

#Working with JSON
input = sc.textFile('../data/testtweet.json')                    
data = input.map(lambda x: json.loads(x))
print(type(data))

#output json data to text file
data.filter(lambda x: x['user']).map(lambda x: json.dumps(x))\
#            .saveAsTextFile('../data/output/jsondump.txt')

#Reading CSV file. Use a function instead of whole file methon in key_value file
def load_record(line):
    """ Parse CSV line """
    line_buffer = io.StringIO(line)
    reader = csv.DictReader(line_buffer)
    return next(reader)
input = sc.textFile('../data/asteroid_data.csv').map(load_record)
print('First 5 records of csv reader: {0}'.format(input.take(5)))
