package ch4;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import scala.Serializable;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Transformation
 *
 * @author 余昕宇
 * @date 2018-06-11 11:59
 **/
public class Transformation {

    public static void main(String[] args) {

        SparkConf sparkConf = new SparkConf().setAppName("transformation");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        JavaRDD<String> lines = sparkContext.textFile("E:\\spark-2.3.0-bin-hadoop2.7\\README.md");
        JavaPairRDD<String, String> input = lines.mapToPair(x -> new Tuple2<>(x.split(" ")[0], x));

        // 1.筛选操作
        JavaPairRDD<String, String> result = input.filter((Function<Tuple2<String, String>, Boolean>) x -> x._2.length() > 5);

        List<Tuple2<String, String>> list = result.collect();

        for (Tuple2<String, String> tmp: list
             ) {
            System.out.println("---------result 1---------- key: " + tmp._1 + " values: " + tmp._2);
        }

        // 2.聚合操作
        List<Tuple2<String, Integer>> listAggregate = Arrays.asList(new Tuple2<>("panda", 1), new Tuple2<>("kafka", 2), new Tuple2<>("panda", 3), new Tuple2<>("kafka", 1));
        JavaPairRDD<String, Integer> inputAggregate = sparkContext.parallelizePairs(listAggregate);

        // 使用mapValues和reduceByKey计算键对应的平均值
        JavaPairRDD<String, Tuple2> resultAggregate = inputAggregate.mapValues(x -> new Tuple2(x, 1)).reduceByKey((x, y) -> new Tuple2((Integer)x._1 + (Integer)y._1, (Integer)x._2 + (Integer)y._2));
        List<Tuple2<String, Tuple2>> tuple2List = resultAggregate.collect();

        for (Tuple2<String, Tuple2> tmp: tuple2List
             ) {
            System.out.println("---------result 2---------- key: " + tmp._1 + " values: " + tmp._2);
        }

        // 使用flatMap、mapToPair和reduceByKey进行单词计数
        JavaRDD<String> words = lines.flatMap(x -> Arrays.asList(x.split(" ")).iterator());
        JavaPairRDD<String, Integer> wordsCount = words.mapToPair(x -> new Tuple2<>(x, 1)).reduceByKey((x, y) -> x + y);

        List<Tuple2<String, Integer>> resultCount = wordsCount.collect();
        for (Tuple2<String, Integer> tmp: resultCount
             ) {
            System.out.println("---------result 3---------- key: " + tmp._1 + " values: " + tmp._2);
        }

        // 使用countByValue进行单词计数
        Map<String, Long> wordCount2 = words.countByValue();
        for (Map.Entry<String, Long> tmp : wordCount2.entrySet()) {
            System.out.println("---------result 4---------- key: " + tmp.getKey() + " values: " + tmp.getValue());
        }

        // 使用combineByKey计算键对应的平均值
        JavaPairRDD<String, AvgCount> stringAvgCountJavaPairRDD = inputAggregate.combineByKey(x -> new AvgCount(x, 1),
                (x, y) -> new AvgCount(x.total + y, x.num + 1),
                (x, y) -> new AvgCount(x.total + y.total, x.num + y.num));

        Map<String, AvgCount> stringAvgCountMap = stringAvgCountJavaPairRDD.collectAsMap();
        for (Map.Entry<String, AvgCount> e: stringAvgCountMap.entrySet()
             ) {
            System.out.println("---------result 5---------- key: " + e.getKey() + " values: " + e.getValue().avg());
        }

        // 自定义并行度
        JavaPairRDD<String, Integer> numPartitions = sparkContext.parallelizePairs(listAggregate).reduceByKey((x, y) -> x + y, 5);
        Map<String, Integer> mapNumPartitions = numPartitions.collectAsMap();
        for (Map.Entry<String, Integer> e: mapNumPartitions.entrySet()
                ) {
            System.out.println("---------result 6---------- key: " + e.getKey() + " values: " + e.getValue());
        }

        // 改变RDD分区数

        // 代价比较大
        System.out.println("---------result 7---------- 原分区数 " + numPartitions.partitions().size());
        numPartitions.repartition(6);
        System.out.println("---------result 8---------- repartition分区数 " + numPartitions.partitions().size());
        // 优化版本
        numPartitions.coalesce(5);
        System.out.println("---------result 9---------- coalesce分区数 " + numPartitions.partitions().size());

        // 3.数据分组

        // 使用groupByKey进行分组
        JavaPairRDD<String, Iterable<Integer>> stringIterableJavaPairRDD = inputAggregate.groupByKey();
        Map<String, Iterable<Integer>> stringIterableMap = stringIterableJavaPairRDD.collectAsMap();
        for (Map.Entry<String, Iterable<Integer>> tmp: stringIterableMap.entrySet()
             ) {
            for (Integer integer : tmp.getValue()) {
                System.out.println("---------result 10---------- key: " + tmp.getKey() + " values: " + integer);
            }
        }

        // 4.连接

        // 内连接
        List<Tuple2<String, String>> listJoin = Arrays.asList(new Tuple2<>("yuxinyu", "tall"),
                new Tuple2<>("hyz", "shaxian"),
                new Tuple2<>("yuxinyu", "guangdong"),
                new Tuple2<>("language", "c"));
        List<Tuple2<String, String>> list2Join = Arrays.asList(new Tuple2<>("yuxinyu", "thin"),
                new Tuple2<>("hyz", "hundun"),
                new Tuple2<>("yuxinyu", "java"));

        JavaPairRDD<String, String> joinPairRDD = sparkContext.parallelizePairs(listJoin);
        JavaPairRDD<String, String> joinPairRDD2 = sparkContext.parallelizePairs(list2Join);
        Map<String, Tuple2<String, String>> innerJoinMap = joinPairRDD.join(joinPairRDD2).collectAsMap();
        for (Map.Entry<String, Tuple2<String, String>> tmp: innerJoinMap.entrySet()
             ) {
            System.out.println("---------result 11---------- key: " + tmp.getKey() + " values: " + tmp.getValue());
        }

        // 左外
        Map<String, Tuple2<String, String>> leftOuterJoinMap = joinPairRDD.join(joinPairRDD2).collectAsMap();
        for (Map.Entry<String, Tuple2<String, String>> tmp: leftOuterJoinMap.entrySet()
                ) {
            System.out.println("---------result 12---------- key: " + tmp.getKey() + " values: " + tmp.getValue());
        }

        // 右外
        Map<String, Tuple2<String, String>> rightOuterJoinMap = joinPairRDD.join(joinPairRDD2).collectAsMap();
        for (Map.Entry<String, Tuple2<String, String>> tmp: rightOuterJoinMap.entrySet()
                ) {
            System.out.println("---------result 12---------- key: " + tmp.getKey() + " values: " + tmp.getValue());
        }

        // 5.数据排序
        List<Integer> integerList = Arrays.asList(3, 5, 1, 7, 8, 2, 10);

        List<Integer> sortedList = sparkContext.parallelize(integerList).sortBy(x -> x, true, 5).collect();
        for (Integer tmp: sortedList) {
            System.out.println("---------result 13---------- Integer List: " + tmp);
        }

        sparkContext.stop();

    }

}

class AvgCount implements Serializable {

    int total;
    int num;

    double avg() {
        return total / (double) num;
    }

    AvgCount(int total, int num) {
        this.total = total;
        this.num = num;
    }

}
