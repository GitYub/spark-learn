package ch4;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Int;
import scala.Tuple1;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Action
 *
 * @author 余昕宇
 * @date 2018-06-11 21:01
 **/
public class Action {

    public static void main(String[] args) {

        SparkConf sparkConf = new SparkConf().setAppName("pair action");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        List<Tuple2<Integer, Integer>> numbers = Arrays.asList(new Tuple2<>(1, 2), new Tuple2<>(3, 4), new Tuple2<>(3, 6));
        JavaPairRDD<Integer, Integer> inputs = sparkContext.parallelizePairs(numbers);

        // countByKey
        Map<Integer, Long> result = inputs.countByKey();
        for (Map.Entry<Integer, Long> entry: result.entrySet()) {
            System.out.println("------------result 1---------- keys: " + entry.getKey() + " values: " + entry.getValue());
        }

        // collectAsMap
        Map<Integer, Integer> resule1 = inputs.collectAsMap();
        for (Map.Entry<Integer, Integer> entry: resule1.entrySet()) {
            System.out.println("------------result 2---------- keys: " + entry.getKey() + " values: " + entry.getValue());
        }

        // lookup
        List<Integer> result3 = inputs.lookup(3);
        for (Integer tmp: result3) {
            System.out.println("------------result 3---------- keys: " + tmp);
        }

        sparkContext.stop();

    }

}
