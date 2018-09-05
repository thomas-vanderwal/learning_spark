import csv 
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("My App")
sc = SparkContext(conf = conf)

with open('../data/near_miss_data.csv') as csv_file: 
    asteroids = csv.DictReader(csv_file)

    ast_dict = {row['neo_reference_id']: row['miss_distance_astronomical'] for row in asteroids}


