/**
 * Created by Chaomin on 6/11/2017.
 */

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

public class Test2 {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("test").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);


        class Sum implements Function2<Integer, Integer, Integer> {
            public Integer call(Integer a, Integer b) { return a + b; }
        }

        JavaRDD<String> lines = sc.textFile("data.txt");
        JavaRDD<Integer> lineLengths = lines.map(new GetLength());
        int totalLength = lineLengths.reduce(new Sum());

        System.out.print(totalLength);

    }

    final static class GetLength implements Function<String, Integer> {
        public Integer call(String s) { return s.length(); }
    }

}
