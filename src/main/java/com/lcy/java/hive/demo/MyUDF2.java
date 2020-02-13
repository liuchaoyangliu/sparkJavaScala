package com.lcy.java.hive.demo;

import org.apache.hadoop.hive.ql.exec.UDF;

public class MyUDF2 extends UDF {
    public MyUDF2() {
    }
    
    public int evaluate(int data) {
        return data + 5;
    }
}
