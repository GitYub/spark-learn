package ch3;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;

import java.util.Arrays;

/**
 * Persistence
 *
 * @author 余昕宇
 * @date 2018-06-10 16:18
 **/
public class Persistence {

    public static void main(String[] args) {

        SparkConf sparkConf = new SparkConf().setAppName("persistence");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<Integer> input = sparkContext.parallelize(Arrays.asList(1, 2, 3, 3, 4, 5));

        JavaRDD<Integer> output = input.map(x -> x * x);
        output.persist(StorageLevel.MEMORY_ONLY());
        System.out.println(output.count());
        System.out.println(StringUtils.join(output.collect(), ","));
        sparkContext.stop();

    }

}
