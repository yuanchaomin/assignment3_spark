import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.*;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;

/**
 * Created by Chaomin on 6/11/2017.
 */
public class Task3 {

    public final String ITEM_SPLIT = "-";

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Example").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // $example on$

        String the_k_itemset_dir = args[0];
        String previous_itemset_dir = args[1];
        String task2_output = args[2];
        String task3_output = args[3];
        String input_data_dir = args[4];

        double confidence_threshold = 0.2;
        if (args.length > 5) {
            confidence_threshold = Double.parseDouble(args[5]);
        }

        JavaRDD<String> data = sc.textFile(the_k_itemset_dir);
        JavaRDD<String> data2 = sc.textFile(previous_itemset_dir);
        JavaRDD<String> GenData = sc.textFile(input_data_dir + "GEO.txt");
        JavaRDD<String> patientData = sc.textFile(input_data_dir + "PatientMetaData.txt");


//        JavaRDD<String> data = sc.textFile("data.txt");
//        JavaRDD<String> data2 = sc.textFile("data2.txt");

        List<String> patientlist = new ArrayList<String>();

        JavaPairRDD<String, String> numpatient = patientData
                .filter(v -> !v.split(",")[0].equals("id"))
                .flatMapToPair(s -> {
                    String[] tem = s.split(",");
                    String[] disease = tem[tem.length - 2].trim().split(" ");
                    String patient = tem[0];
                    ArrayList<Tuple2<String, String>> record = new ArrayList<Tuple2<String, String>>();
                    for (String d : disease) {
                        record.add(new Tuple2<String, String>(patient, d));
                    }
                    return record.iterator();
                }).filter(v -> v._2().trim().equals("breast-cancer") || v._2().trim().equals("prostate-cancer") ||
                        v._2().trim().equals("pancreatic-cancer") || v._2().trim().equals("leukemia") ||
                        v._2().trim().equals("lymphoma")).reduceByKey((v1, v2) -> v1 + ";" + v2).sortByKey();

        List<Tuple2<String, String>> temp = numpatient.collect();

        for (int i = 0; i < temp.size(); i++) {
            patientlist.add(temp.get(i)._1());
        }

        Broadcast<List<String>> sharedata = sc.broadcast(patientlist);

        JavaPairRDD<String, String> Genrecord = GenData.filter(v -> !v.split(",")[0].equals("patientid") && Double.parseDouble(v.split(",")[2]) > 1250000)
                //.filter(v->v.split(",")[1].equals("42")&&Double.parseDouble(v.split(",")[2])>1250000)
                .filter(m -> {
                    String tem = m.split(",")[0];
                    return sharedata.value().stream().anyMatch(str -> str.equals(tem));
                }).mapToPair(s -> {
                    String[] tem = s.split(",");
                    return new Tuple2<String, String>(tem[0], tem[1]);
                }).reduceByKey((v1, v2) -> v1 + ";" + v2).sortByKey();

        int len = Genrecord.collect().size();

        JavaRDD<String> itemsets_subset = data
                .map(new add_subset())
                .flatMap(new flat_subset());

        JavaRDD<Tuple2<String, Integer>> sub_freq_itemsets = data2
                .map(new Function<String, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> call(String line) throws Exception {

                        List<String> record = Arrays.asList(line.replaceAll("\\s+|\\(|\\)", "").split(","));
                        String sub_itemset = record.get(1);
                        int count = Integer.parseInt(record.get(0));

                        return new Tuple2<>(sub_itemset, count);
                    }
                });


        JavaPairRDD<String, String> data2_pair = data2
                .mapToPair(new PairFunction<String, String,String>() {
                    @Override
                    public Tuple2<String, String> call(String line) throws Exception {
                        List<String> record = Arrays.asList(line.replaceAll("\\s+|\\(|\\)", "").split(","));
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

        JavaRDD<String> result = Joined_data
                .map(new Function<Tuple2<String, Tuple2<String, String>>, Tuple4<String, String, String, String>>() {
                    @Override
                    public Tuple4<String, String, String, String> call(Tuple2<String, Tuple2<String, String>> input) throws Exception {

                        List<String> record = Arrays.asList(input.toString().replaceAll("\\s+|\\(|\\)","").split(","));

                        String sub_itemset = record.get(0);
                        String itemset = record.get(1);

                        String elementsB[] = sub_itemset.split(";");
                        String elementsA[] = itemset.split(";");

                        Set<String> set_A = new HashSet<String>(Arrays.asList(elementsA));
                        Set<String> set_B = new HashSet<String>(Arrays.asList(elementsB));

                        set_A.removeAll(set_B);
                        String result = set_A
                                .toString()
                                .replaceAll("\\s+|\\[|\\]","")
                                .replaceAll(",",";");

                        String result2 = result + ";";

                        int itemset_count = Integer.parseInt(record.get(2));
                        int sub_itemset_count = Integer.parseInt(record.get(3));

                        double confidence_value = ((double)itemset_count)/sub_itemset_count;

                        Double toBeTruncated = confidence_value;

                        Double truncatedDouble = BigDecimal.valueOf(toBeTruncated)
                                .setScale(3, RoundingMode.HALF_UP)
                                .doubleValue();

                        return new Tuple4<>(itemset,sub_itemset,result2, Double.toString(truncatedDouble));

                    }
                }).filter(v -> Double.parseDouble(v._4()) > 0.6)
                .map(new Function<Tuple4<String, String, String, String>, String>() {
                    @Override
                    public String call(Tuple4<String, String, String, String> line) throws Exception {
                        String sub_itemset = line._2();
                        String sub_itemset_completmentset = line._3();
                        String confidence_value = line._4();

                        String result_string = sub_itemset + "\t"
                                                + sub_itemset_completmentset + "\t"
                                                +confidence_value;

                        return result_string;
                    }
                });

        JavaPairRDD<String,String> Pair_result = result
                .mapToPair(new PairFunction<String, String, String>() {
                    @Override
                    public Tuple2<String, String> call(String input) throws Exception {
                        String[] record = input.split("\t");
                        String sub_itemset = record[0];
                        String sub_itemset_completmentset = record[1];
                        String confidence_value = record[2];


                        return new Tuple2<>(confidence_value, sub_itemset+","+sub_itemset_completmentset);
                    }
                });

        JavaPairRDD<String,String> swapped_result = Pair_result
                .sortByKey(false,1)
                .mapToPair(new PairFunction<Tuple2<String, String>, String, String>() {
                    @Override
                    public Tuple2<String, String> call(Tuple2<String, String> input) throws Exception {
                        String cofidence_value = input._1();
                        String sub_itemset_and_its_completmentset = input._2();
                        return new Tuple2<>(sub_itemset_and_its_completmentset, cofidence_value);
                    }
                });

        JavaRDD<String> task3_result = swapped_result
                .map(new Function<Tuple2<String, String>, String>() {
                    @Override
                    public String call(Tuple2<String, String> input) throws Exception {
                        String  sub_itemset = input._1().split(",")[0];
                        String sub_itemset_completmentset = input._1().split(",")[1];
                        String confidence_value = input._2();

                        String result_string = sub_itemset + "\t"
                                + sub_itemset_completmentset + "\t"
                                +confidence_value;

                        return result_string;
                    }
                });


        JavaPairRDD<String,String> task2_pair_data = data
                   .mapToPair(new PairFunction<String, String, String>() {
                       @Override
                       public Tuple2<String, String> call(String input) throws Exception {
                           List<String> record = Arrays.asList(input.replaceAll("\\s+|\\(|\\)","").split(","));
                           String[] gen_item = record.get(1).split(";");
                           String result = String.join("\t", gen_item);

                           double support_rate = ((double)Integer.parseInt(record.get(0)))/len;

                           Double toBeTruncated = support_rate;

                           Double truncatedDouble = BigDecimal.valueOf(toBeTruncated)
                                   .setScale(3, RoundingMode.HALF_UP)
                                   .doubleValue();

                           return new Tuple2<String,String>(truncatedDouble.toString(), result);
                       }
                   });

        JavaRDD<String> task2_result = task2_pair_data
                .map(new Function<Tuple2<String, String>, String>() {
                    @Override
                    public String call(Tuple2<String, String> input) throws Exception {
                        //List<String> record = Arrays.asList(input.toString().replaceAll("\\s+|\\(|\\)","").split(","));
                        String support_count = input._1();
                        String item_in_itemset = input._2();

                        String result = support_count + "\t"
                                        + item_in_itemset;

                        return result;

                    }
                });


        data_pair.foreach(line -> {
            System.out.println(line);
        });
//
//        System.out.println(len);



//        data2_pair.foreach(line -> {
//            System.out.println(line);
//        });
//
//        task3_result.foreach(line -> {
//            System.out.println(line);
//        });

//        Pair_result.foreach(line -> {
//            System.out.println(line);
//        });

        task2_result.saveAsTextFile(task2_output);
        task3_result.saveAsTextFile(task3_output);



    }

    final static class add_subset implements Function<String, Tuple3<String, List<String>, Integer>>{
        @Override
        public Tuple3<String, List<String>, Integer> call(String line) throws Exception {
            List<String> record = Arrays.asList(line.replaceAll("\\s+|\\(|\\)","").split(","));

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
        final String ITEM_SPLIT = ";";

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
