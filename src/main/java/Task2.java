
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.util.*;

/**
 * Created by yangyizhou on 2017/6/10.
 */
public class Task2 {


    public static void main(String[] args) {

        String input = args[0];
        String output = args[1];
        String output2 = args[2];
        JavaPairRDD<String, Integer> iteration = null;
        JavaPairRDD<String, Integer> temresult = null;
        JavaPairRDD<String, String> temlist = null;
        JavaRDD<String> temread = null;

        List<String> translist = new ArrayList<String>();
        List<String> insidelist = new ArrayList<String>();
        List<String> iterationlist = new ArrayList<String>();


        int k = 2; //defult size = 3
        double support = 0.3; //defule support rate = 0.3

        if (args.length == 4) {
            int inputK = Integer.parseInt(args[3]);
            k = inputK;
        }
        if (args.length == 5) {
            double inputsup = Double.parseDouble(args[4]);
            support = inputsup;
        }


        SparkConf conf = new SparkConf();
        conf.setAppName("Frequent Itemset Mining").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> GenData = sc.textFile(input + "GEO.txt");
        JavaRDD<String> patientData = sc.textFile(input + "PatientMetaData.txt");


//                GenData.foreach(line -> {
//            System.out.println(line);
//        });

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

        System.out.println(len + "........");


        double finalSupport = support;
        JavaPairRDD<String,Integer> frequentOne = Genrecord.flatMapToPair(s->{
            String[] tem = s._2().split(";");
            ArrayList<Tuple2<String,Integer>> result = new ArrayList<Tuple2<String,Integer>>();
            for(String t:tem){
                result.add(new Tuple2<String,Integer>(t+"-",1));
            }
            return result.iterator();
        }).reduceByKey((v1,v2)->v1+v2).sortByKey().filter(s->(float)s._2()/(float) len > finalSupport);

        JavaPairRDD<Integer,String> out1 = frequentOne.mapToPair(s->{
            String[] t = s._1().split("-");
            return new Tuple2<Integer,String>(s._2,t[0]+";");
        }).sortByKey(false);

//        numpatient.foreach(line -> {
//            System.out.println(line);
//        });

        out1.saveAsTextFile(output+"1");

        System.out.println("111111111111111111111111111");


        List<Tuple2<String, Integer>> trans = frequentOne.collect();


        for(int i =0;i<trans.size();i++){
            translist.add(trans.get(i)._1());
        }


        for(int i=0;i<translist.size();i++){
            for(int j=0;j<translist.size();j++){
                iterationlist.add(translist.get(i)+translist.get(j));
                //System.out.println(iterationlist.get(i));
            }
        }
        Broadcast<List<String>> broaditeration = sc.broadcast(iterationlist);
        Broadcast<List<String>> broadOneList = sc.broadcast(translist);


        for(int x=0;x<k;x++){

            // System.out.println(iterationlist.size());

            double finalSupport1 = support;
            Broadcast<List<String>> finalBroaditeration = broaditeration;
            int finalX = x;
            iteration  = Genrecord.flatMapToPair(s->{
                String[] t = s._2().split(";");
                ArrayList<Tuple2<String,Integer>> result = new ArrayList<Tuple2<String,Integer>>();
                for(String a: finalBroaditeration.value()){
                    int tnum = 0;
                    for(int z=0;z<a.split("-").length;z++){
                        if(Arrays.asList(t).contains(a.split("-")[z])){
                            tnum++;
                            //System.out.println(tnum);
                        }
                    }
                    if(tnum==a.split("-").length){
                        //System.out.println(result.size());
                        result.add(new Tuple2<String,Integer>(a,1));
                    }
                }
                return result.iterator();
            }).mapToPair(s->{
                String[] tem = s._1().split("-");
                String result = "";
                int[] sort = new int[tem.length];
                for(int m=0;m<tem.length;m++){
                    sort[m]=Integer.parseInt(tem[m]);
                }
                Arrays.sort(sort);
                for(int v:sort){
                    result = result+v+"-";
                }
                return new Tuple2<String,Integer>(result,1);

            }).reduceByKey((v1,v2)->v1+v2)
                    .mapToPair(s->{
                        int repeat = 1;
                        for(int i=1;i<=(finalX +2);i++){
                            repeat=repeat*i;
                        }
                        return new Tuple2<String,Integer>(s._1(),s._2()/repeat);
                    })
                    .filter(s->(float)s._2()/(float) len > finalSupport1 &&!s._1().equals("-"))
                    .filter(s->{
                        String[] t = s._1().split("-");
                        Set<String> set = new HashSet<String>();
                        for(String a:t){
                            set.add(a);
                        }
                        if(set.size()<t.length) {
                            return false;
                        }else{
                            return true;
                        }

                    });


            trans = iteration.collect();
            if(trans.size()==0){
                break;
            }

            JavaPairRDD<Integer,String> out2 = iteration.mapToPair(s->{
                String[] t = s._1().split("-");
                String out = "";
                for(String a:t){
                    out = out+a +";";
                }
                return new Tuple2<Integer,String>(s._2,out);
            }).sortByKey(false);

            if ((x+2) < k ){
                //out2.saveAsTextFile(output+(x+2));
                out2.saveAsTextFile(output+ (x+2));
            }
            else{
                out2.saveAsTextFile(output2+ "task2_result")
                ;
            }


            System.out.println("2222222222222222222222222222" + x);
            temresult=iteration;

            iterationlist.clear();
            insidelist.clear();

//            temread = sc.textFile(output+"/"+(x+2));
//            JavaPairRDD<String,String> temreadlist = temread.mapToPair(s->{
//                String tem = s.substring(1,s.length()-1);
//                String[] t=tem.split(",");
//                System.out.println(t[0]+"................................");
//                return new Tuple2<String,String>(t[0],t[1]);
//            });
            //temreadlist.saveAsTextFile(output+"000"+(x+2));

//             List<Tuple2<String,String>> temtrans = new ArrayList<Tuple2<String,String>>();
            trans = iteration.collect();


            if(x==k-1){
                break;
            }


            for(int i =0;i<trans.size();i++){
                insidelist.add(trans.get(i)._1());
            }


            for(int i=0;i<insidelist.size();i++){
                for(int j=0;j<broadOneList.value().size();j++){
                    iterationlist.add(insidelist.get(i)+broadOneList.value().get(j));
                    //System.out.println(insidelist.get(i)+broadOneList.value().get(j));
                    //System.out.println(broadOneList.value().get(j));

                }
            }
            System.out.println("#33333333333333333333");

            broaditeration = sc.broadcast(iterationlist);





        }
    }






}
