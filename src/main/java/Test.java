import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

import java.util.*;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;
import scala.Tuple3;

/**
 * Created by Chaomin on 6/11/2017.
 */
public class Test {

    public final String ITEM_SPLIT = "-";

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("FP-growth Example").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // $example on$
        JavaRDD<String> data = sc.textFile("data.txt");
        JavaRDD<String> data2 = sc.textFile("data2.txt");

        JavaRDD<String> itemsets_subset = data
                .map(new add_subset())
                .flatMap(new flat_subset());

        JavaRDD<Tuple2<String, Integer>> sub_freq_itemsets = data2
                .map(new Function<String, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> call(String line) throws Exception {

                        List<String> record = Arrays.asList(line.split(","));
                        String sub_itemset = record.get(1);
                        int count = Integer.parseInt(record.get(0));

                        return new Tuple2<>(sub_itemset, count);
                    }
                });


        JavaPairRDD<String, String> data2_pair = data2
                .mapToPair(new PairFunction<String, String,String>() {
                    @Override
                    public Tuple2<String, String> call(String line) throws Exception {
                        List<String> record = Arrays.asList(line.replaceAll("\\s+", "").split(","));
                        String sub_itemset = record.get(1);
                        String count = record.get(0);

                        return new Tuple2<>(sub_itemset, count);
                    }
                });

        JavaPairRDD<String,String> data_pair = itemsets_subset
                .mapToPair(new PairFunction<String, String, String>() {
                    @Override
                    public Tuple2<String,String> call(String line) throws Exception {
                        List<String> record = Arrays.asList(line.replaceAll("\\s+|\\(|\\)","").split(","));
                        String itemset = record.get(0);
                        String sub_itemset = record.get(1);
                        Integer count = Integer.parseInt(record.get(2));

                        return new Tuple2<>(sub_itemset, new Tuple2<>(itemset, count).toString());

                    }
                });

        JavaPairRDD<String,Tuple2<String,String>> Joined_data = data_pair.join(data2_pair);

//        Joined_data.foreach(line -> {
//            System.out.println(line);
//        });

        JavaRDD<Tuple3<String,String,Double>> result = Joined_data
                .map(new Function<Tuple2<String, Tuple2<String, String>>, Tuple3<String, String, Double>>() {
                    @Override
                    public Tuple3<String, String, Double> call(Tuple2<String, Tuple2<String, String>> input) throws Exception {

                        List<String> record = Arrays.asList(input.toString().replaceAll("\\s+|\\(|\\)","").split(","));

                        String sub_itemset = record.get(0);
                        String itemset = record.get(1);

                        String elementsB[] = sub_itemset.split("-");
                        String elementsA[] = itemset.split("-");

                        Set<String> set_A = new HashSet<String>(Arrays.asList(elementsA));
                        Set<String> set_B = new HashSet<String>(Arrays.asList(elementsB));

                        set_A.removeAll(set_B);
                        String result = set_A
                                .toString()
                                .replaceAll("\\s+|\\[|\\]","")
                                .replaceAll(",","-");

                        String result2 = result + "-";

                        int itemset_count = Integer.parseInt(record.get(2));
                        int sub_itemset_count = Integer.parseInt(record.get(3));

                        double confidence_value = ((double)itemset_count)/sub_itemset_count;

                        return new Tuple3<>(sub_itemset,result2,confidence_value);

                    }
                });

        result.foreach(line -> {
            System.out.println(line);
        });




    }

    final static class add_subset implements Function<String, Tuple3<String, List<String>, Integer>>{
        @Override
        public Tuple3<String, List<String>, Integer> call(String line) throws Exception {
            List<String> record = Arrays.asList(line.replaceAll("\\s+","").split(","));

            int count = Integer.parseInt(record.get(0));
            String itemset = record.get(1);
            List<String> subset_list = subset(itemset);

            return new Tuple3<>(itemset, subset_list, count);

        }
    }

    final static class flat_subset implements FlatMapFunction<Tuple3<String, List<String>, Integer>, String> {

        @Override
        public Iterator<String> call(Tuple3<String, List<String>, Integer> input_tuple3) throws Exception {

            int count = input_tuple3._3();
            String itemset = input_tuple3._1();
            List<String> itemset_subset_list = input_tuple3._2();

            List<String> result = new ArrayList<>();

            for(String itemset_subset: itemset_subset_list) {

                result.add(new Tuple3<>(itemset, itemset_subset, count).toString());
            }

            return result.iterator();
        }
    }


    final static class  Generate_Subset implements Function<String, List<String>> {

        public  List<String> call(String itemsets) {

            return subset(itemsets);
        }
    }

    private static List<String> subset(String sourceSet) {
        final String ITEM_SPLIT = "-";

        List<String> result = new ArrayList<>();

        String[] strings = sourceSet.split(ITEM_SPLIT);
        for(int i=1;i<(int)(Math.pow(2, strings.length))-1;i++) {

            String item = "";
            String flag = "";
            int ii=i;
            do {

                flag += ""+ii%2;
                ii = ii/2;
            } while (ii>0);
            for(int j=flag.length()-1;j>=0;j--) {

                if(flag.charAt(j)=='1') {

                    item = strings[j]+ITEM_SPLIT+item;
                }
            }
            result.add(item);
        }

        return result;
    }

    public static void printRDD(final JavaRDD<List<String>> s) {
        s.foreach(System.out::println);
    }

}
