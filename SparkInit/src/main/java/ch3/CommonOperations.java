package ch3;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Map;

/**
 * CommonOperations
 *
 * @author 余昕宇
 * @date 2018-06-10 14:03
 **/
public class CommonOperations {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("common operations");
        JavaSparkContext sparkContext = new JavaSparkContext(conf);

        // 1.针对各个元素的操作

        // 用map为每一个元素求平方
        JavaRDD<Integer> input = sparkContext.parallelize(Arrays.asList(1, 2, 3, 4, 5));
        JavaRDD<Double> res = input.map(x -> Math.pow(x, 2));

        System.out.println("-----------------1"+StringUtils.join(res.collect(), ","));

        // 用flatmap将行切分
        JavaRDD<String> lines = sparkContext.textFile("E:\\spark-2.3.0-bin-hadoop2.7\\README.md");
        JavaRDD<String> words = lines.flatMap(s -> Arrays.asList(s.split(" ")).iterator());

        System.out.println("-----------------2"+StringUtils.join(words.collect(), ","));

        // 2.伪集合操作
        JavaRDD<Integer> numbers = sparkContext.parallelize(Arrays.asList(1, 2, 3, 3));

        // map
        JavaRDD<Integer> map = numbers.map(x -> x + 1);
        System.out.println("-----------------3"+StringUtils.join(map.collect(), ","));

        // filter
        JavaRDD<Integer> filter = numbers.filter(x -> x != 3);
        System.out.println("-----------------4"+StringUtils.join(filter.collect(), ","));

        // distinct
        JavaRDD<Integer> distinct = numbers.distinct();
        System.out.println("-----------------5"+StringUtils.join(distinct.collect(), ","));

        // sample
        JavaRDD<Integer> sample = numbers.sample(false, 0.5);
        System.out.println("-----------------6"+StringUtils.join(sample.collect(), ","));

        JavaRDD<Integer> numbers2 = sparkContext.parallelize(Arrays.asList(3, 4, 5));

        // union
        JavaRDD<Integer> union = numbers.union(numbers2);
        System.out.println("-----------------7"+StringUtils.join(union.collect(), ","));

        // intersection
        JavaRDD<Integer> intersection = numbers.intersection(numbers2);
        System.out.println("-----------------8"+StringUtils.join(intersection.collect(), ","));

        // subtract
        JavaRDD<Integer> subtract = numbers.subtract(numbers2);
        System.out.println("-----------------9"+StringUtils.join(subtract.collect(), ","));

        // cartesian
        JavaPairRDD<Integer, Integer> cartesian = numbers.cartesian(numbers2);
        System.out.println("-----------------10"+StringUtils.join(cartesian.collect(), ","));

        // 3.行动操作
        // reduce
        System.out.println("-----------------10"+numbers.reduce((x, y) -> x + y));

        // map+reduce求平均值
        JavaRDD<Tuple2<Integer, Integer>> average = numbers.map(x -> new Tuple2<>(x, 1));
        Tuple2<Integer, Integer> averageRes = average.reduce((x, y) -> new Tuple2<>(x._1 + y._1, x._2 + y._2));
        System.out.println("-----------------11"+averageRes._1 / averageRes._2);

        // aggregate求平均值
        AvgCount aggregateRes = numbers.aggregate(new AvgCount(0, 0),
                (a, x) -> new AvgCount(a.total + x, a.num + 1),
                (a, b) -> new AvgCount(a.total + b.total, a.num + b.num)
        );
        System.out.println("-----------------12"+aggregateRes.avg());

        // collect
        System.out.println("-----------------13"+StringUtils.join(numbers.collect(), ","));

        // count
        System.out.println("-----------------14"+numbers.count());

        // countByValue
        Map<Integer, Long> a = numbers.countByValue();
        for (Map.Entry<Integer, Long> b : a.entrySet()) {
            System.out.println(b.getKey() + ": " + b.getValue());
        }

        // take
        System.out.println("-----------------15"+StringUtils.join(numbers.take(2), ","));

        // top
        System.out.println("-----------------16"+StringUtils.join(numbers.top(2), ","));

        // takeOrdered
        System.out.println("-----------------17"+StringUtils.join(numbers.takeOrdered(2), ","));

        // takeSample
        System.out.println("-----------------18"+StringUtils.join(numbers.takeSample(false, 2), ","));

        // reduce
        System.out.println("-----------------19"+numbers.reduce((x, y) -> x + y));

        // fold
        System.out.println("-----------------20"+numbers.fold(0, (x, y) -> x + y));

        // aggregate
        System.out.println("-----------------21"+numbers.aggregate(new AvgCount(0, 0),
                (aa, x) -> new AvgCount(aa.total + x, aa.num + 1),
                (aa, b) -> new AvgCount(aa.total + b.total, aa.num + b.num)
        ).avg());

        // foreach 省略

        // 创建DoubleRDD
        JavaDoubleRDD doubleRDD = numbers.mapToDouble(x -> Math.pow(x, 2));
        System.out.println("-----------------22"+doubleRDD.mean());

        sparkContext.stop();

    }

}

class AvgCount implements Serializable {

    int total;
    int num;

    AvgCount(int total, int num) {
        this.total = total;
        this.num = num;
    }

    double avg() {
        return total / (double) num;
    }
}
