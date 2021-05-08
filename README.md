# BIG DATA COMPUTING 20/21 (Prof. Pietracaprina and Silvestri)
## Home Work 1 (G26HW1.py)
1. Reads the input set of reviews into an RDD of strings called RawData (each review is read as a single string), and subdivides it into K partitions.
2. Transform the RDD RawData into an RDD of pairs (String,Float) called normalizedRatings, so that for each string of RawData representing a review (ProductID,UserID,Rating,Timestamp), NormalizedRatings contains the pair (ProductID,NormRating), where NormRating=Rating-AvgRating and AvgRating is the average rating of all reviews by the user "UserID". To accomplish this step you can safely assume that there are a few reviews for each user. Note that normalizedRatings may contain several pairs for the same product, one for each existing review for that product!
3. Transform the RDD normalizedRatings into an RDD of pairs (String,Float) called maxNormRatings which, for each ProductID contains exactly one pair (ProductID, MNR) where MNR is the maximum normalized rating of product "ProductID". The maximum should be computed either using the reduceByKey method or the mapPartitionsToPair/mapPartitions method. (Hint: get inspiration from the WordCountExample program).
4. Print the T products with largest maximum normalized rating, one product per line. (Hint: use a combination of sortByKey and take methods.)


## Home Work 2 (G26HW2.py)
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

