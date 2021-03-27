from pyspark import SparkContext, SparkConf, conf
from pyspark.sql import SparkSession
sc = SparkContext()
spark = SparkSession(sparkContext=sc)
import sys
import os
import random as rand





def main():
    #check for ars in the cmd
    assert len(sys.argv)==4, "python3 G26HW1.py <K> <T> <csvFileName>"

    #Spark Setup
    conf = SparkConf().setAppName('G26HW1').setMaster("Local[*]")
    #sc =SparkContext(conf=conf)

    #Read inputs 
    #Read k from the argv
    K =sys.argv[1]
    assert K.isdigit(), "K Should be an int"
    K = int(K)
    print(K)
    #Read The T from the argv
    T =sys.argv[2]
    assert T.isdigit(), "T Should be an int"
    T = int(T)
    print(T)
    #Read the file path from the argv
    data_path = sys.argv[3]
    assert os.path.isfile(data_path), "File or folder not found"
    csv_file = spark.read.csv(data_path)
    csv_file.show()


if __name__ == "__main__":
	main()



