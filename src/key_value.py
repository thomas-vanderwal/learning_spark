import csv 
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("My App")
sc = SparkContext(conf = conf)

# Read near miss data with DictReader and then iterate through the rows creating key value records.
with open('../data/near_miss_data.csv') as csv_file:
    asteroids = csv.DictReader(csv_file)
    key_val= {'{0},{1}'.format(row['neo_reference_id'],row['miss_distance_astronomical']) for row in asteroids}

# Create RDD of key-value pairs
data = sc.parallelize(key_val)
print(data.take(10))
