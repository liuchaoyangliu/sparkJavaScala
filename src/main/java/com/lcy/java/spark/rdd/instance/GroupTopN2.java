package com.lcy.java.spark.rdd.instance;

import org.apache.commons.collections.IteratorUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.*;


public class GroupTopN2 {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("sparkStudy").setMaster("local");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        jsc.setLogLevel("ERROR");

        JavaRDD<String> lines = jsc.textFile("file:\\D:\\sparkData\\demo.txt");

        JavaPairRDD<String, Integer> pairs = lines.mapToPair(line -> {
            String[] lineSplited = line.split(",");
            return new Tuple2<>(lineSplited[0], Integer.valueOf(lineSplited[1]));
        });

        JavaPairRDD<String, Iterable<Integer>> groupedPairs = pairs.groupByKey();

        JavaPairRDD<String, Iterable<Integer>> top3Score = groupedPairs.mapToPair(
                classScores -> {

                    Integer[] top3 = new Integer[3];

                    String className = classScores._1;
                    Iterator<Integer> scores = classScores._2.iterator();

                    while (scores.hasNext()) {
                        Integer score = scores.next();
                        for (int i = 0; i < 3; i++) {
                            if (top3[i] == null) {
                                top3[i] = score;
                                break;
                            } else if (score > top3[i]) {
                                for (int j = 2; j > i; j--) {
                                    top3[j] = top3[j - 1];
                                }
                                top3[i] = score;
                                break;
                            }
                        }
                    }
                    return new Tuple2<>(className, Arrays.asList(top3));
                });

        top3Score.foreach(t -> {
            System.out.println("class: " + t._1 + "  " + t._2);
//            class: class3  [14, 11, 8]
//            class: class4  [15, 12, 9]
//            class: class2  [13, 10, 7]
        });

        jsc.close();
    }
    
    
    
    
    
    public void demo() {
        SparkConf conf = new SparkConf().setAppName("sparkStudy").setMaster("local");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        jsc.setLogLevel("ERROR");

        jsc.textFile("file:\\D:\\sparkData\\demo.txt")
                .mapToPair(line -> {
                    String[] lineSplited = line.split(",");
                    return new Tuple2<>(lineSplited[0], Integer.valueOf(lineSplited[1]));
                })
                .groupByKey()
                .mapToPair(
                        classScores -> {
                            String className = classScores._1;
                            List<Integer> list = IteratorUtils.toList(classScores._2.iterator());
                            Collections.sort(list, (o1, o2) -> o1 - o2);
                            return new Tuple2<>(className, list.subList(0, 3));
                        })
                .foreach(t -> System.out.println("class: " + t._1 + " " + t._2));

        jsc.close();
    }


}


//        class3[2, 11, 14]
//        class4[12, 15, 6]
//        class2[1, 13, 7]
