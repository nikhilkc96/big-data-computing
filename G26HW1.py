import os
import sys
from pyspark import SparkContext, SparkConf


def Get_Pairs(pair):
	pairs = {}
	Sum = 0
	for p in pair[1]:
		ProductID, Rating = p[0]
		Sum += Rating
	avg = Sum / len(pair[1])

	for p in pair[1]:
		ProductID, Rating = p[0]
		if ProductID not in pairs.keys():
			pairs[ProductID] = [Rating-avg]
		else:
			pairs[ProductID].append(Rating-avg)
	return [(key, pairs[key]) for key in pairs.keys()]

def Rating_By_User(data):
	pairs = {}
	Sum_Rating = {}
	for datas in data.split("\n"):
		line = datas.split(",")
		ProductID, UserID, Rating= str(line[0]), str(line[1]), float(line[2])
		if UserID not in pairs.keys():
			pairs[UserID] = []
			pairs[UserID].append((ProductID, Rating))
			Sum_Rating[UserID] = Rating
    # RDD String (RawData) to RDD Pair(normalizedRatings)
	for key, value in pairs.items():
		for pair in range(0, len(value)):
			if len(value) > 1:
				pairs[key][pair] = (pairs[key][pair][0], pairs[key][pair][1] - (Sum_Rating[key] / len(value)))
	return [(key, pairs[key]) for key in pairs.keys()]

#fucntion to find maxNormRating
def maxNormRatings(RawData):
	maxNormRating = (RawData.flatMap(Rating_By_User).groupByKey().flatMap(Get_Pairs).reduceByKey(lambda x, y: max(x, y)).map(lambda x: (x[1], x[0])))
	return maxNormRating

def main():
    #check for ars in the cmd
    assert len(sys.argv) == 4, "python3 G26HW1.py <K> <T> <csvFileName>"

    #Spark Setup
    conf = SparkConf().setAppName('G26HW1').setMaster("local[*]")
    sc =SparkContext(conf=conf)
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
    print("INPUT PARAMETERS: K="+str(K)+", T="+str(T)+", "+"file="+str(data_path)+"\n")
    print('OUTPUT:')

    for maxNormRating, ProductID in maxNormRatings(RawData).sortByKey(False).take(T):
	    print("ProductID", ProductID, "maxNormRating", maxNormRating)

if __name__ == "__main__":
	main()
