import os
import sys
from pyspark import SparkContext, SparkConf
from pyspark.ml.clustering import KMeans
import math
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml.feature import VectorAssembler




def strToTuple(line):
    ch = line.strip().split(",")
    point = tuple(float(ch[i]) for i in range(len(ch)-1))
    return (point, int(ch[-1]))  # returns (point, cluster_index)


def geo_dist(lat1, lon1,lat2,lon2):
    R = 6372.8
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 -lon1)
    a = math.sin(dphi/2)**2 + math.cos(phi1) * math.cos(phi2)*math.sin(dlambda/2)**2
    return 2*R*math.atan2(math.sqrt(a), math.sqrt(1 - a))

def main():
    #check for ars in the cmd
    assert len(sys.argv) == 4, "python3 G26HW2.py <K> <csvFileName>"

    #Spark Setup
    conf = SparkConf().setAppName('G26HW2').setMaster("local[*]")
    sc = SparkContext(conf=conf)
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
    fullClustering = sc.textFile(data_path).map(strToTuple)
    #Partition of the data given
    fullClustering = fullClustering.repartition(numPartitions=K)
    print("Number of Partitions ::", fullClustering.getNumPartitions())
    print("Number of Samples :: ", fullClustering.count())
    print("INPUT PARAMETERS: K="+str(K)+", T=" +
          str(T)+", "+"file="+str(data_path)+"\n")
    print('OUTPUT:')
    

    

   


if __name__ == "__main__":
    main()

