import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import scala.Tuple2;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

public class G26HW2  {
    public static Tuple2<Vector, Integer> strToTuple (String str){
        String[] tokens = str.split(",");
        double[] data = new double[tokens.length];
        for (int i = 0; i < tokens.length-1; i++) {
            data[i] = Double.parseDouble(tokens[i]);
        }
        Vector point = (Vector) Vectors.dense(data);
        Integer cluster = Integer.valueOf(tokens[tokens.length-1]);
        Tuple2<Vector, Integer> pair = new Tuple2<>(point, cluster);
        return pair;
    }
    public  static  void  main(String[] args) throws IOException {

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
        JavaPairRDD<Vector,Integer> fullClustering = sc.textFile(file).mapToPair(x -> strToTuple(x)).repartition(8).cache();
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        //                PART 2
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        Broadcast<Map<Integer, Long>> sharedClusterSizes = sc.broadcast(fullClustering.map(x -> x._2).countByValue());
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        //                PART 3
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        Broadcast<List<Tuple2<Vector, Integer>>> clusteringSample = sc.broadcast(fullClustering.filter((x) -> {
            return ThreadLocalRandom.current().nextInt() <= Math.min(T/sharedClusterSizes.getValue().get(x._2).doubleValue(), 1); }).collect());
        


    }
}