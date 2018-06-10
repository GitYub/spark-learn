package ch3;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

/**
 * CreateRDD
 *
 * @author 余昕宇
 * @date 2018-06-10 13:03
 **/
public class CreateRDD {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("create RDD");
        JavaSparkContext sparkContext = new JavaSparkContext(conf);

        // way1
        JavaRDD<String> lines = sparkContext.parallelize(Arrays.asList("pandas", "hello kafka", "apple"));

        // way2
        JavaRDD<String> lines2 = sparkContext.textFile("E:\\spark-2.3.0-bin-hadoop2.7\\README.md");

    }

}
