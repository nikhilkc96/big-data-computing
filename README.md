# BIG DATA COMPUTING 20/21 (Prof. Pietracaprina and Silvestri)
## Home Work 1 (G26HW1.py)
1. Reads the input set of reviews into an RDD of strings called RawData (each review is read as a single string), and subdivides it into K partitions.
2. Transform the RDD RawData into an RDD of pairs (String,Float) called normalizedRatings, so that for each string of RawData representing a review (ProductID,UserID,Rating,Timestamp), NormalizedRatings contains the pair (ProductID,NormRating), where NormRating=Rating-AvgRating and AvgRating is the average rating of all reviews by the user "UserID". To accomplish this step you can safely assume that there are a few reviews for each user. Note that normalizedRatings may contain several pairs for the same product, one for each existing review for that product!
3. Transform the RDD normalizedRatings into an RDD of pairs (String,Float) called maxNormRatings which, for each ProductID contains exactly one pair (ProductID, MNR) where MNR is the maximum normalized rating of product "ProductID". The maximum should be computed either using the reduceByKey method or the mapPartitionsToPair/mapPartitions method. (Hint: get inspiration from the WordCountExample program).
4. Print the T products with largest maximum normalized rating, one product per line. (Hint: use a combination of sortByKey and take methods.)


## Home Work 2 (G26HW2.java)
The purpose of Homework 2 is to demonstrate that when an exact analysis is too costly (hence, unfeasible for large inputs), resorting to careful approximation strategies might yield a substantial gain in performance at the expense of a limited loss of accuracy. The homework will focus on the estimation of the silhouette coefficient of a clustering. Review the theory in Slides on Clustering (Part 3). Before describing the assignment, it is necessary to discuss a few issues.

Representation of points. We will work with points in Euclidean space (real cooordinates) and with the squared Euclidean L2-distance.

**FOR JAVA USERS**. In Spark, points can be represented as instances of the class org.apache.spark.mllib.linalg.Vector and can be manipulated through static methods offered by the class org.apache.spark.mllib.linalg.Vectors. For example, method Vectors.dense(x) transforms an array x of double into an instance of class Vector, while method Vectors.sqdist(x,y) computes the (d(x,y))^2 between two Vector x and y, where "d(.,.)" is the standard Euclidean L2-distance. Details on these classes can be found in the Spark Java API. Warning. Make sure to use the classes from the org.apache.spark.mllib package. There are classes with the same name in org.apache.spark.ml package which are functionally equivalent, but incompatible with those of the org.apache.spark.mllib package.

**FOR PYTHON USERS**. We suggest to represent points as the standard tuple of float (i.e., point = (x1, x2, ...)). Although Spark provides the class Vector also for Python (see pyspark.mllib package), its performance is very poor and its more convenient to use tuples, especially for points from low-dimensional spaces.

Time measurements. Measuring times when using RDDs in Spark requires some care, due to the lazy evaluation mechanism, namely the fact that RDD transformations are executed only when an action (e.g., counting the number of elements of the RDD) requires the transformed data. Please read what is written about this issue in the dedicated section of the Spark Programming Guide.

**Broadcast variables**. When read-only global data declared in the main program must be used by an RDD transformation (e.g., by map, flatMap or flatMapToPair methods) it is convenient to declare them as broadcast variables, which Spark distributes efficiently to the workers executing the transformation. Please read what is written about this issue in the dedicated section of the Spark Programming Guide.

**ASSIGNMENT**. You must write a program GxxHW2.java (for Java users) or GxxHW2.py (for Python users), where xx is your two-digit group number, which receives in input, as command-line arguments, the following data (in this ordering)

A path to a text file containing point set in Euclidean space partitioned into k clusters. Each line of the file contains, separated by commas, the coordinates of a point and the ID of the cluster (in [0,k-1]) to which the point belongs. E.g., Line 1.3,-2.7,3 represents the point (1.3,-2.7) belonging to Cluster 3. Your program should make no assuptions on the number of dimensions!
The number of clusters k (an integer).
The expected sample size per cluster t (an integer).
### The program must do the following:

1. Read the input data. In particular, the clustering must be read into an RDD of pairs (point,ClusterID) called fullClustering which must be cached and partitioned into a reasonable number of partitions, e.g., 4-8. (Hint: to this purpose, you can use the code and the suggestions provided in the file Input.java, for Java users, and Input.py, for Python users).
2. Compute the size of each cluster and then save the k sizes into an array or list represented by a Broadcast variable named sharedClusterSizes. (Hint: to this purpose it is very convenient to use the RDD method countByValue() whose description is found in the Spark Programming Guide)
3. Extract a sample of the input clustering, where from each cluster C, each point is selected independently with probability min{t/|C|, 1} (Poisson Sampling). Save the sample, whose expected size is at most t*k, into a local structure (e.g., ArrayList in java or list in Python) represented by a Broadcast variable named clusteringSample. (Hint: the sample can be extracted with a simple map operation on the RDD fullClustering, using the cluster sizes computed in Step 2).
4. Compute the approximate average silhouette coefficient of the input clustering and assign it to a variable approxSilhFull. (Hint: to do so, you can first transform the RDD fullClustering by mapping each element (point, clusterID) of fullClustering to the approximate silhouette coefficient of 'point' computed as explained here exploiting the sample, and taking the average of all individual approximate silhouette coefficients). 
5. Compute (sequentially) the exact silhouette coefficient of the clusteringSample and assign it to a variable exactSilhSample.
6. Print the following values: (a) value of approxSilhFull, (b) time to compute approxSilhFull (Step 4),  (c) value of exactSilhSample, (d) time to compute exactSilhSample (Step 5). Times must be in ms. Use the following output format
Test your program using the following input clusterings computed on pointsets in R^2 which represent Uber pickups in New York City (if you want to learn more about the datasets click here)

Uber_3_small.csv: 1012 points subdivided into k=3 clusters.\
Uber_3_large.csv: 1028136 points subdivided into k=3 clusters.\
Uber_10_large.csv: 1028136 points subdivided into k=10 clusters.

and fill the table given in this word file with the results of the experiments indicated in the document.


**OUTPUT**
Value of approxSilhFull = 0.56370 \
Time to compute approxSilhFull = 1902 ms \
Value of exactSilhSample = 0.59216 \
Time to compute exactSilhSample = 812 ms





## Home Work 3 (G26HW3.java)

The purpose of this homework is to run a Spark program on the CloudVeneto cluster available for the course. The objective of the program is the selection of a suitable number of clusters for a given input dataset using the silhouette coefficient. The program must do so by pipelining the Spark implementation of Lloyd's algorithm and the approximation of the silhouette developed for Homework 2. In the homework you will test the scalability of the various steps of the pipeline.

Using CloudVeneto
A brief description of the cluster available for the course, together with instructions on how to access the cluster and how to run your program on it are given in this User guide for the cluster on CloudVeneto.

Spark implementation of Lloyd's algorithm.
In the RDD-based API of the mllib package, Spark provides an implementation of LLoyd's algorithm for k-means clustering. In particular, the algorithm is implemented by method train of class KMeans which receives in input the points stored as an RDD of Vector, in Java, and of NumPy arrays in Python, the number k of clusters, and the number of iterations. The method computes an initial set of centers using, as a default, algorithm kmeans|| (a parallel variant of kmeans++), and then executes the specified number of iterations. As output the method returns the final set of centers, represented as an instance of class KMeansModel. For this latter class, method clusterCenters will return the centers as an array of Vector (in Java) or list of NumPy arrays in Python. Refer to the official Spark documentation on clustering (RDD-based API) for more details.

Assignment
You must write a program GxxHW3.java (for Java users) or GxxHW3.py (for Python users), where xx is your two-digit group number, which receives in input, as command-line arguments, the following data (in this ordering)

A path to a text file containing a point set in Euclidean space. Each line of the file contains the coordinates of a point separated by spaces. (Your program should make no assumptions on the number of dimensions!)
An integer kstart which is the initial number of clusters.
An integer h which is the number of values of k that the program will test.
An integer iter which is the number of iterations of Lloyd's algorithm.
An integer M which is the expected size of the sample used to approximate the silhouette coefficient.
An integer L which is the number of partitions of the RDDs containing the input points and their clustering.
The program must do the following (recycle pieces of code from Homework 2 where appropriate):

Reads the various parameters passed as command-line arguments. In particular, the set of points must be stored into an RDD called inputPoints, which must be cached and subdivided into L partitions. Two notices: (a) in the input file the coordinates of the points are separated by spaces and not by commas as in Homework 2, so take this into account where adapting the reading method used in Homework 2; (b) the textfile method invoked from the Spark context, which you will use to read the input textfile into an RDD, is able to read gzipped files as well.  After reading the parameters print the time spent to read the input points.
For every k between kstart and kstart+h-1 does the following
Computes a clustering of the input points with k clusters, using the Spark implementation of Lloyd's algorithm described above with iter iterations. The clustering must be stored into an RDD currentClustering of pairs (point, cluster_index) with as many elements as the input points. The RDD must be cached and partitioned into L partitions. (If computed by transforming each element of inputPoints with a map method, it should inherit its partitioning.)
Computes the approximate average silhouette coefficient of the clustering stored in the RDD currentClustering using the approximation algorithm implemented and tested in Homework 2, with t=M/k. In particular, the approximate silhouette coefficient of each point must be computed using a sample obtained by selecting min{t,|C|} points from each cluster C, in expectation.
Prints the following values: (a) the value k; (b) the value of the approximate average silhouette coefficient; (c) the time spent to compute the clustering; (d) the time spent to compute the silhouette (which must include the time to extract the sample).Times must be in ms.
Use the output format given in this example (kstart=5, h=3).

IMPORTANT: To define the Spark configuration in your program, use the following instructions:

(Java):
SparkConf conf = new SparkConf(true)
.setAppName("Homework3")
.set("spark.locality.wait", "0s")
(Python):
conf = (SparkConf().setAppName('Homework3').set('spark.locality.wait','0s'))
(The option spark.locality.wait must be set as indicated to avoid that for medium size datasets Spark uses less than the specified number of executors.) Also, do not set the master (setMaster option). This option is preconfigured on CloudVeneto.

Test your program in local mode on your PC to make sure that it runs correctly. For this local test you can use this dataset. For a description of the datasets used in the homework refer to this page.

Test your program on the cluster using the datasets which have been preloaded in the cluster. Use various configurations of parameters and report your results using the the table given in this word file.

WHEN USING THE CLUSTER, YOU MUST STRICTLY FOLLOW THESE RULES:

To avoid congestion, groups with even (resp., odd) group number must use the clusters in even (resp., odd) days.
Do not run several instances of your program at once.
Do not use more than 16 executors.
Try your program on a smaller dataset first.
Remember that if your program is stuck for more than 1 hour, its execution will be automatically stopped by the system.
SUBMISSION INSTRUCTIONS. Each group must submit a zipped folder GxxHW3.zip, where xx is your ID group. The folder must contain the program (GxxHW3.java or GxxHW3.py) and a file GxxHW3table.docx with containing the aforementioned table. Only one student per group must do the submission using the link provided in the Homework3 section. Make sure that your code is free from compiling/run-time errors and that you comply with the specification, otherwise your score will be penalized.


scp build/libs/big-data-computing-all.jar  username@login.dei.unipd.it:.

ssh chidananda@login.dei.unipd.it

scp -P 2222 big-data-computing-all.jar  group26@147.162.226.106:.

ssh -p 2222 group26@147.162.226.106

hdfs dfs -ls /data/BDC2021

spark-submit --num-executors 2  --class G26HW3 big-data-computing-all.jar /data/BDC2021/synt2M.txt.gz 5 3 10 16 500