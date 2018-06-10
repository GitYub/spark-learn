package ch2;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * ch2.WordCount
 *
 * @author 余昕宇
 * @date 2018-06-10 11:10
 **/
public class WordCount {

    public static void main(String[] args) {

        SparkConf sparkConf = new SparkConf().setAppName("wordcount");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> input = sparkContext.textFile("E:\\spark-2.3.0-bin-hadoop2.7\\README.md");
        JavaRDD<String> words = input.flatMap(
                (FlatMapFunction<String, String>) s -> Arrays.asList(s.split(" ")).iterator()
        );

        JavaPairRDD<String, Integer> counts = words.mapToPair(
                (PairFunction<String, String, Integer>) x -> (new Tuple2<>(x, 1))
        ).reduceByKey((Function2<Integer, Integer, Integer>) (x, y) -> x + y);

        List<Tuple2<String, Integer>> output = counts.collect();
        for (Tuple2<?,?> tuple : output) {
            System.out.println(tuple._1() + ": " + tuple._2());
        }

        sparkContext.stop();

    }

}
