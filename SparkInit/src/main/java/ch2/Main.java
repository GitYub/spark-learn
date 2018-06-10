package ch2;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * ch2.Main
 *
 * @author 余昕宇
 * @date 2018-06-10 11:01
 **/
public class Main {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setMaster("local").setAppName("My App");
        JavaSparkContext sparkContext = new JavaSparkContext(conf);

        sparkContext.stop();

    }

}
