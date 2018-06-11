package ch4;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * CreatePairRDD
 *
 * @author 余昕宇
 * @date 2018-06-11 11:40
 **/
public class CreatePairRDD {

    public static void main(String[] args) {

        SparkConf sparkConf = new SparkConf().setAppName("create pair rdd");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        // 从外部文件系统
        JavaRDD<String> lines = sparkContext.textFile("E:\\spark-2.3.0-bin-hadoop2.7\\README.md");
        JavaPairRDD<String, String> pairLines = lines.mapToPair(x -> new Tuple2<>(x.split(" ")[0], x));
        List<Tuple2<String, String>> list = pairLines.collect();

        for (Tuple2<String, String> tmp: list
             ) {
            System.out.println("key: " + tmp._1 + " value: " + tmp._2);
        }

        // 从内存数据集中
        List<Tuple2<Integer, Integer>> tuple2List = Arrays.asList(new Tuple2<>(1, 2), new Tuple2<>(2, 3));
        JavaPairRDD<Integer, Integer> pairRDD = sparkContext.parallelizePairs(tuple2List);
        System.out.println("---------result " + pairRDD.collect());

        sparkContext.stop();

    }

}
