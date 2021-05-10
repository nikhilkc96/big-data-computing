import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import scala.Tuple2;

import java.io.IOException;
import java.util.*;

public class G26HW2  {
    public static Tuple2<Vector, Integer> strToTuple (String str){
        String[] tokens = str.split(",");
        double[] data = new double[tokens.length];
        for (int i = 0; i < tokens.length-1; i++) {
            data[i] = Double.parseDouble(tokens[i]);
        }
        Vector point = Vectors.dense(data);
        Integer cluster = Integer.valueOf(tokens[tokens.length-1]);
        Tuple2<Vector, Integer> pair = new Tuple2<>(point, cluster);
        return pair;
    }
    public static void main(String[] args) throws IOException {

        //Sets Spark configuration
        SparkConf conf = new SparkConf(true).setAppName("G26HW2");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("WARN");

        //Check the args count
        if(args.length != 3){
            throw new IllegalArgumentException("Usage: Check the Args:: <CSV_File> <K> <T>");
        }
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        //                PART 1
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        //Read the args array
        String file = args[0];
        int K =  Integer.parseInt(args[1]);
        int T = Integer.parseInt(args[2]);
        Random rand = new Random();
        long size = sc.textFile(file).cache().count();
        JavaPairRDD<Vector,Integer> fullClustering = sc.textFile(file).mapToPair(x -> strToTuple(x)).repartition(8).cache();
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        //                PART 2
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        Broadcast<Map<Integer, Long>> sharedClusterSizes = sc.broadcast(fullClustering.map(x -> x._2).countByValue());
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        //                PART 3
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        Broadcast<List<Tuple2<Vector, Integer>>> clusteringSample = sc.broadcast(fullClustering.filter((x) -> {
            return rand.nextDouble() <= Math.min(T/sharedClusterSizes.getValue().get(x._2).doubleValue(), 1); }).collect());
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        //                PART 4
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        long time_approx_start =System.currentTimeMillis();
        JavaRDD<Double> Score =fullClustering.map((x) ->{
            double[] sum = new double[K];
            double[] par= new double[K];
            Vector ClusterPoint = x._1;
            int ClusterIndex =x._2;
            for( Tuple2<Vector, Integer> tuple2 : clusteringSample.getValue()){
                Vector ClusterPoint2 = tuple2._1;
                int ClusterIndex2 =tuple2._2;
                sum[ClusterIndex2] += Vectors.sqdist(ClusterPoint,ClusterPoint2);

            }
            for(int i=0;i<K;i++) {
                par[i] = Math.min(T/sharedClusterSizes.getValue().get(x._2).doubleValue(), 1);
                for(int j=0;j<K;j++){
                    sum[j] *= par[j];
                }
            }
            double approx1 = sum[ClusterIndex];
            double approx2 = Double.MAX_VALUE;
            for (int i=0;i<K;i++){
                if(i !=ClusterIndex) {
                    approx2 = Math.min(par[i] / Math.min(T, sharedClusterSizes.getValue().get(i).doubleValue()), approx2);
                }
            }
            return (approx2-approx1)/Math.max(approx1,approx2)/size;
        });
        double approxSilhFull =Score.reduce((x,y) -> x+y);
        long time_approx_end =System.currentTimeMillis();


        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        //                PART 5
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&


        long time_exact_start =System.currentTimeMillis();
        int[] SamclustersSize = new int[K];
        double exactSilhSample = 0;
        for (Tuple2<Vector, Integer> tuple : clusteringSample.getValue()) {
            int clusterIndex = tuple._2;
            SamclustersSize[clusterIndex] = SamclustersSize[clusterIndex] + 1;
        }
        for( Tuple2<Vector, Integer> tuple1 : clusteringSample.getValue()) {
            Vector ClusterPoint = tuple1._1;
            int ClusterIndex = tuple1._2;
            double[] sum = new double[K];
            for (Tuple2<Vector, Integer> tuple2 : clusteringSample.getValue()) {
                Vector ClusterPoint2 = tuple2._1;
                int ClusterIndex2 = tuple2._2;
                sum[ClusterIndex2] += Vectors.sqdist(ClusterPoint, ClusterPoint2);
            }
            double approx1 = SamclustersSize[ClusterIndex];
            double approx2 = Double.MAX_VALUE;
            for (int i = 0; i < K; i++) {
                if (i != ClusterIndex) {
                    approx2 = Math.min(SamclustersSize[i] / Math.min(T, sharedClusterSizes.getValue().get(i).doubleValue()), approx2);
                }
            }
            exactSilhSample = approx2 - approx1 / Math.max(approx1, approx2) / clusteringSample.getValue().size();
        }
            long time_exact_end = System.currentTimeMillis();
            // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
            //                PART 6
            // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
            System.out.println("Value of approxSilhFull :: " + approxSilhFull);
            System.out.println("Time to compute approxSilhFull :: " + (time_approx_end - time_approx_start) + "ms");
            System.out.println("Value of exactSilhSample :: " + exactSilhSample);
            System.out.println("Time to compute exactSilhSample :: " + (time_exact_end - time_exact_start) + "ms");
        }
    }
