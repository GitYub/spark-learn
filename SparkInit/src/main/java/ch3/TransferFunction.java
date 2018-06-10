package ch3;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

/**
 * TransferFunction
 *
 * @author 余昕宇
 * @date 2018-06-10 13:46
 **/
public class TransferFunction {

    public static void main(String[] args) {

        SparkConf sparkConf = new SparkConf().setAppName("transfer function");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> input = sparkContext.textFile("E:\\spark_test_data\\log.txt");

        // 匿名内部类进行函数传递
        JavaRDD<String> errors = input.filter(
                new Function<String, Boolean>() {
                    @Override
                    public Boolean call(String s) {
                        return s.contains("error");
                    }
                }
        );

        // 具名类进行函数传递
        JavaRDD<String> errors1 = input.filter(new ContainsError());

        // 带参数的Java函数类
        JavaRDD<String> errors2 = input.filter(new Contains("error"));

        // lambda表达式
        JavaRDD<String> errors3 = input.filter(x -> x.contains("error"));

    }

}

class ContainsError implements Function<String, Boolean> {

    @Override
    public Boolean call(String s) {
        return s.contains("error");
    }
}

class Contains implements Function<String, Boolean> {

    private String query;

    public Contains(String query) {
        this.query = query;
    }

    @Override
    public Boolean call(String s) {
        return s.contains(query);
    }
}
