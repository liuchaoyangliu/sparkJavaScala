package com.lcy.java.hive.demo;

import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

public class MyUDTF2 extends GenericUDTF {
    private List<String> dataList = new ArrayList();
    
    public MyUDTF2() {
    }
    
    public StructObjectInspector initialize(StructObjectInspector argOIs) throws UDFArgumentException {
        List<String> fieldNames = new ArrayList();
        fieldNames.add("word1");
        fieldNames.add("word2");
        List<ObjectInspector> fieldOIs = new ArrayList();
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }
    
    public void process(Object[] args) throws HiveException {
        String data = args[0].toString();
        String[] fields = data.split("-");
        String[] words1 = fields[0].split(",");
        String[] words2 = fields[1].split(",");
        this.dataList.clear();
        this.dataList.add(words1[0]);
        this.dataList.add(words1[1]);
        this.forward(this.dataList);
        this.dataList.clear();
        this.dataList.add(words2[0]);
        this.dataList.add(words2[1]);
        this.forward(this.dataList);
    }
    
    public void close() throws HiveException {
    }
}

