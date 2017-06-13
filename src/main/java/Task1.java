
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import com.sun.org.apache.regexp.internal.RE;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;
/**
 * Created by yangyizhou on 2017/5/30.
 */
public class Task1 {
    public static void main(String[] args) {
        String input = args[0];
        String output = args[1];

        SparkConf conf = new SparkConf();
        conf.setAppName("Count measurements conducted per researcher").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> GenData = sc.textFile(input+"GEO.txt" );
        JavaRDD<String> patientData = sc.textFile(input+"PatientMetaData.txt");

        JavaPairRDD<String, String> numpatient = patientData
                .filter(v->!v.split(",")[0].equals("id"))
                .flatMapToPair(s->{
                    String[] tem = s.split(",");
                    String[] disease = tem[tem.length-2].trim().split(" ");
                    String patient = tem[0];
                    ArrayList<Tuple2<String,String>> record = new ArrayList<Tuple2<String,String>>();
                    for(String d:disease){
                        record.add(new Tuple2<String,String>(patient,d));
                    }
                    return record.iterator();
                }).filter(v->v._2().trim().equals("breast-cancer")||v._2().trim().equals("prostate-cancer")||
                        v._2().trim().equals("pancreatic-cancer")||v._2().trim().equals("leukemia")||
                        v._2().trim().equals("lymphoma"));


        JavaPairRDD<String,Double> Genrecord = GenData.filter(v->!v.split(",")[0].equals("id"))
                .filter(v->v.split(",")[1].equals("42")&&Double.parseDouble(v.split(",")[2])>1250000)
                .mapToPair(s->{
                    //System.out.println(s);
                    String[] tem = s.split(",");
                    Double v = Double.parseDouble(tem[2]);
                    return new Tuple2<String,Double>(tem[0],v);
                });

        JavaPairRDD<String,Tuple2<String,Double>> joinresult = numpatient.join(Genrecord);

        JavaPairRDD<String,Integer> record = joinresult.mapToPair(s->{
            String dis = s._2()._1();
            return new Tuple2<String,Integer>(dis,1);
        }).reduceByKey((v1,v2)->v1+v2).sortByKey()
                .mapToPair(s->{
                    int a= s._2();
                    String b =s._1;
                    return new Tuple2<Integer,String>(a,b);
                }).sortByKey(false)
                .mapToPair(s->{
                    int a =s._1();
                    String b = s._2();
                    return new Tuple2<String,Integer>(b,a);
                });


        record.saveAsTextFile(output);




        sc.close();

    }
}
