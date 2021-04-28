try:
    sc.stop()
except:
    pass
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
sc=SparkContext()
spark = SparkSession(sparkContext=sc)

rdd = sc.textFile("/Users/nikhilkc/Desktop/BDC/data.txt")
rdd.take(5)