import csv 
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("My App")
sc = SparkContext(conf = conf)
sc.setLogLevel('OFF')

# Read near miss data with DictReader and then iterate through the rows creating key value records.
with open('../data/near_miss_data.csv') as csv_file:
    near_miss_data = csv.DictReader(csv_file)
    near_miss_kv = {(row['neo_reference_id'], float(row['miss_distance_astronomical']))
                    for row in near_miss_data}

# Read over asteroid data with DictReader.
with open('../data/asteroid_data.csv') as csv_file:
    asteroid_data = csv.DictReader(csv_file)
    asteroid_kv = {(row['neo_reference_id'],
                    (float(row['absolute_magnitude_h']) if row['absolute_magnitude_h'] != '' else 0,
                     float(row['min_orbit_intersection']))) for row in asteroid_data}

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

# Using combineByKey to find th per-key average. Here we will use near miss data
# to find the average miss distance of each asteroid

# Returns a new key-valu RDD with (asteroid, (sum miss distance, count near misses))
sum_count = nm_data.combineByKey((lambda x: (x, 1)),
                                 (lambda x, y: (x[0] + y, x[1] + 1)),
                                 (lambda x, y: (x[0] + y[0], x[1] + y[1])))

results = sum_count.map(lambda x: (x[0], x[1][0]/x[1][1]))
print('First 10 asteroids and their average miss distance: {0}'.format(results.take(10)))

# Count the times an asteroid has a near miss
cnt = nm_data.map((lambda x: (x[0], 1))).reduceByKey(lambda x, y: (x+y))

# Join this count RDD to our PHA RDD to find how many times each PHA has came into close
# Contact with an orbiting body. Results ordered by most near misses
joined_data = cnt.join(pha_data)
print('Top 10 PHA asteroids by number of near misses: {}'
      .format(joined_data.takeOrdered(10, key=lambda x: -x[1][0])))
