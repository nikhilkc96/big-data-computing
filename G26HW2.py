import os
import sys
from pyspark import SparkContext, SparkConf
import math
import random
from pyspark.mllib.linalg import Vectors
from pyspark.mllib.linalg import Vector


def strToTuple(line):
    ch = line.strip().split(",")
    point = tuple(float(ch[i]) for i in range(len(ch)-1))
    return (point, int(ch[-1]))  # returns (point, cluster_index)

def main():
    #check for ars in the cmd
    assert len(sys.argv) == 4, "python3 G26HW2.py <K> <csvFileName>"

    #Spark Setup
    conf = SparkConf().setAppName('G26HW2').setMaster("local[*]")
    sc = SparkContext(conf=conf)
    sc.setLogLevel("WARN")
    #Read inputs
    #Read k from the argv
    K = sys.argv[1]
    assert K.isdigit(), "K Should be an int"
    K = int(K)
    #Read The T from the argv
    T = sys.argv[2]
    assert T.isdigit(), "T Should be an int"
    T = int(T)
    #Read the file path from the argv
    data_path = sys.argv[3]
    assert os.path.isfile(data_path), "File or folder not found"
    fullClustering = sc.textFile(data_path)
    fullClustering = fullClustering.map(
        lambda x: strToTuple(x)).repartition(8).cache()
    
    clusterSizes = fullClustering.map(lambda x:x).countByValue()
    print(clusterSizes)
    sharedClusterSizes = sc.broadcast(clusterSizes)
    # clusteringSampleRDD = 

    print("Number of Partitions ::", fullClustering.getNumPartitions())
    print("Number of Samples :: ", fullClustering.count())
    print("INPUT PARAMETERS: K="+str(K)+", T=" +
          str(T)+", "+"file="+str(data_path)+"\n")
    print('OUTPUT:')
    

    

   


if __name__ == "__main__":
    main()

