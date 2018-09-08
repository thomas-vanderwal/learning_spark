import csv 
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("My App")
sc = SparkContext(conf = conf)
sc.setLogLevel('OFF')

# Read near miss data with DictReader and then iterate through the rows creating key value records.
with open('../data/near_miss_data.csv') as csv_file:
    near_miss_data = csv.DictReader(csv_file)
    near_miss_kv= {(row['neo_reference_id'],float(row['miss_distance_astronomical'])) for row in near_miss_data}

# Read over asteroid data with DictReader.
with open('../data/asteroid_data.csv') as csv_file:
    asteroid_data = csv.DictReader(csv_file)
    asteroid_kv = {(row['neo_reference_id'],(float(row['absolute_magnitude_h']) if len(row['absolute_magnitude_h']) > 0  else 0
                                            ,float(row['min_orbit_intersection']))) for row in asteroid_data}

ast_data = sc.parallelize(asteroid_kv).persist()

# Create RDD of key-value pairs
nm_data = sc.parallelize(near_miss_kv).persist()
print("Total number of records in Data: {0}".format(nm_data.count()))

# Groupging by neo reference ID
grp_data = nm_data.groupByKey()
print("Total number of records after grouping by key: {0}".format(grp_data.count()))

# Simple Filter. Return list of potentially hazardous asteroids based on criteria
#  -- Absolutel Magnitude < 22
#  -- Minimum Orbital Intersection (MOID) < .05 AU
pha_data = ast_data.filter(lambda x: x[1][0] < 22.0 and  x[1][1] < .05)
print('First 10 potentially hazardous asteroids: {0}'.format(pha_data.take(10)))
