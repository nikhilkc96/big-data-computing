import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.spark_project.guava.collect.Iterables;
import scala.Tuple2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class G26HW3 {

    public static void main(String[] args) throws IOException {
        //Sets Spark configuration
        SparkConf conf = new SparkConf(true).setAppName("Homework3").set("spark.locality.wait", "0s");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("WARN");
        //Check the args count
        if (args.length != 6) {
            throw new IllegalArgumentException("Usage: Check the Args:: <TXT_File> <KStart> <H> <Iter> <M> <L>");
        }
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        //                PART 1
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        //Read the args array
        ArrayList<Long> clusterSizes = new ArrayList<>();
        ArrayList<Float> minProb = new ArrayList<>();
        List<Tuple2<Vector, Integer>> selectedSamples = new ArrayList<>();

        String file = args[0];
        int KStart = Integer.parseInt(args[1]);
        int H = Integer.parseInt(args[2]);
        int Iter = Integer.parseInt(args[3]);
        int M = Integer.parseInt(args[4]);
        int L = Integer.parseInt(args[5]);
        long size = sc.textFile(file).count();
        long read_time_start = System.currentTimeMillis();
        JavaRDD<Vector> inputPoints = sc.textFile(file).repartition(L).map(s -> {
            String[] arr = s.split(" ");
            double[] values = new double[arr.length];
            for (int i = 0; i < arr.length; i++) {
                values[i] = Double.parseDouble(arr[i]);
            }
            return Vectors.dense(values);
        }).cache();
        long read_time_end = System.currentTimeMillis();

        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        //                PART 2
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        for (int k = KStart; k <= KStart + H - 1; k++) {
            final int finalK = k;
            long read_time_cl_start = System.currentTimeMillis();
            //Lloyds algorithm
            KMeansModel clusters = KMeans.train(inputPoints.rdd(), k, Iter);
            JavaPairRDD<Vector, Integer> currentClustering = inputPoints.repartition(L).flatMapToPair(point-> {
                List<Tuple2<Vector, Integer>> pointPairs = new ArrayList<>() ;
                pointPairs.add(new Tuple2<>(point, clusters.predict(point)));
                return pointPairs.iterator();
            }).cache();
            long read_time_cl_end = System.currentTimeMillis();

            Broadcast<Map<Integer, Long>> sharedClusterSizes =
                    sc.broadcast(currentClustering.map(Tuple2::_2).countByValue());

            Broadcast<List<Tuple2<Vector, Integer>>> clusteringSample = sc.broadcast(
                    currentClustering
                            .filter(data -> Math.random() <
                                    Math.min((float) (M / finalK) / sharedClusterSizes.value().get(data._2), 1)
                            )
                            .mapToPair(pair -> new Tuple2<>(pair._2, pair._1))
                            .groupByKey()
                            .mapValues(iterator -> Iterables.limit(iterator, (M / finalK)))
                            .flatMapValues(iterable -> iterable)
                            .mapToPair(pair -> new Tuple2<>(pair._2, pair._1))
                            .collect()
            );

            long time_approx_start = System.currentTimeMillis();
            JavaRDD<Double> Score = currentClustering.map((x) -> {
                double[] sum = new double[finalK];
                double[] par = new double[finalK];
                Vector ClusterPoint = x._1;
                int ClusterIndex = x._2;
                for(int i = 0; i < finalK; i++)
                    par[i] = 1/Double.min((M / finalK), sharedClusterSizes.value().get(i).doubleValue());
                for (Tuple2<Vector, Integer> tuple2 : clusteringSample.getValue()) {
                    Vector ClusterPoint2 = tuple2._1;
                    int ClusterIndex2 = tuple2._2;
                    sum[ClusterIndex2] += Vectors.sqdist(ClusterPoint,ClusterPoint2);
                }
                for(int i=0;i<finalK;i++)
                    sum[i] *= par[i];
                double approx1 =sum[ClusterIndex];
                double approx2 = Double.MAX_VALUE;
                for (int i = 0; i < finalK; i++) {
                    if (i != ClusterIndex && approx2 > sum[i]) {
                        approx2 = sum[i];
                    }
                }
                return (approx2 - approx1) / Math.max(approx1, approx2) / size;
            });
            double approxSilhFull = Score.reduce((x, y) -> x + y);
            long time_approx_end = System.currentTimeMillis();

            System.out.println("Time for input reading = " + (read_time_end - read_time_start));
            System.out.println("Number of Clusters K :: " + k);
            System.out.println("Silhouette Coefficient :: " + approxSilhFull);
            System.out.println("Time for Clustering :: " + (read_time_cl_end-read_time_cl_start));
            System.out.println("Time for Silhouette Computation :: " + (time_approx_end-time_approx_start)+ "\n");

        }
      }
    }
