package ch3;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.util.List;


/**
 * Transformation
 *
 * @author 余昕宇
 * @date 2018-06-10 13:11
 **/
public class Transformation {

    public static void main(String[] args) {

        SparkConf sparkConf = new SparkConf().setAppName("transformation");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> input = sparkContext.textFile("E:\\spark_test_data\\log.txt");
        JavaRDD<String> errorLines = input.filter((Function<String, Boolean>) x -> x.contains("error"));

        List<String> list = errorLines.collect();
        for (String a: list
             ) {
            System.out.println(a);
        }

        System.out.println("union操作");

        JavaRDD<String> warningLines = input.filter((Function<String, Boolean>) x -> x.contains("warning"));
        JavaRDD<String> badLines = errorLines.union(warningLines);

        list = badLines.collect();
        for (String a: list
                ) {
            System.out.println(a);
        }

        sparkContext.stop();

    }

}
