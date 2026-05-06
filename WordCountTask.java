package spark.batch.tp21;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.SparkConf;
import scala.Tuple2;
import java.util.Arrays;

public class WordCountTask {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("WordCountTask").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile(args[0]);
        JavaRDD<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
        JavaPairRDD<String, Integer> wordPairs = words.mapToPair(word -> new Tuple2<>(word, 1));
        JavaPairRDD<String, Integer> wordCounts = wordPairs.reduceByKey((a, b) -> a + b);

       wordCounts.coalesce(1).saveAsTextFile(args[1]);

        sc.close();
    }
}
