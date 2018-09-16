import sys
from pyspark import SparkConf, SparkContext


if __name__ == '__main__':
    if len(sys.argv) != 4:
        print("Error usage: JobScript [sparkmaster] [input file] [output file]")
        sys.exit(-1)

    job_name = sys.argv[0]
    master = sys.argv[1]
    input_file = sys.argv[2]
    output_file = sys.argv[3]
 
    sc = SparkContext(master, job_name)
 
