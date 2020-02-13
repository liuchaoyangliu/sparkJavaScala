package com.lcy.java.spark.demo;

import scala.actors.threadpool.Arrays;

import java.util.Iterator;

public class Demo {
    
    public static void main(String[] args) {
        
        Integer[] top3 = new Integer[3];
        Integer[] integers = {12,45,78,23,12,45,23,14,25,89};
        Iterator<Integer> scores = Arrays.asList(integers).iterator();
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
    
        for (int i = 0; i < top3.length; i++) {
            System.out.println(top3[i]);
        }
    
    }
    
}
