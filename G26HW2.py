import os
import sys
from pyspark import SparkContext, SparkConf


def main():
    #check for ars in the cmd
    assert len(sys.argv) == 4, "python3 G26HW2.py <K> <T> <csvFileName>"

    #Spark Setup
    conf = SparkConf().setAppName('G26HW1').setMaster("local[*]")
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
    RawData = sc.textFile(data_path)
    #Partition of the data given
    RawData = RawData.repartition(numPartitions=K)
    print("Number of Partitions ::", RawData.getNumPartitions())
    print("Number of Samples :: ", RawData.count())
    print("INPUT PARAMETERS: K="+str(K)+", T=" +
          str(T)+", "+"file="+str(data_path)+"\n")
    print('OUTPUT:')

   


if __name__ == "__main__":
	main()
