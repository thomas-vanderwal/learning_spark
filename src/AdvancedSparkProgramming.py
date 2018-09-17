""" Working document for Chapter 6 concepts """
import sys
import csv
import math
from pyspark import SparkConf, SparkContext

def extract_call_signs(line):
    global blank_lines
    if line == '':
        blank_lines += 1
    return line.split(' ')

def load_call_lookup():
    with open('../data/call_signs_tbl.txt', 'r') as f:
        return f.readlines()

if __name__ == '__main__':
    if len(sys.argv) != 4:
        print("Error usage: JobScript [sparkmaster] [input file] [output file]")
        sys.exit(-1)

    job_name = sys.argv[0]
    master = sys.argv[1]
    input_file = sys.argv[2]
    output_file = sys.argv[3]

    sc = SparkContext(master, job_name)

    # Create an acucmulator initialized to 0
    #  This will be used to count the number of empty lines in the file
    blank_lines = sc.accumulator(0)
    file = sc.textFile(input_file)

    call_signs = file.flatMap(extract_call_signs)

    call_signs.count() # Call an action so blank lines can be displayed
    print('Blank lines: {0}'.format(blank_lines.value))

    # Load call signs lookup table into a broadcast variable
    sign_prefixes = sc.broadcast(load_call_lookup)
    print(type(sign_prefixes))

    ###################################################
    #### Numeric Stats. Switching to my NeoWs Data ####
    ###################################################
    with open('../data/near_miss_data.csv') as csv_file:
        near_miss_data = csv.DictReader(csv_file)
        near_miss_distance = {(float(row['miss_distance_astronomical']))
                              for row in near_miss_data}
    nm_data = sc.parallelize(near_miss_distance).persist()
    stats = nm_data.stats()
    stdev = stats.stdev()
    mean = stats.mean()

    print('Total number of near missses: {0}'.format(nm_data.count()))
    #remove outliers
    reasonable_distances = nm_data.filter(lambda x: math.fabs(x - mean) < 3 * stdev)
    print('Total number after outliers removed: {0}'.format(reasonable_distances.count()))
