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

public class MyUDTF extends GenericUDTF {
    private List<String> dataList = new ArrayList();
    
    public MyUDTF() {
    }
    
    public StructObjectInspector initialize(StructObjectInspector argOIs) throws UDFArgumentException {
        List<String> fieldNames = new ArrayList();
        fieldNames.add("word");
        List<ObjectInspector> fieldOIs = new ArrayList();
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }
    
    public void process(Object[] args) throws HiveException {
        String data = args[0].toString();
        String splitKey = args[1].toString();
        String[] words = data.split(splitKey);
        String[] var5 = words;
        int var6 = words.length;
        
        for(int var7 = 0; var7 < var6; ++var7) {
            String word = var5[var7];
            this.dataList.clear();
            this.dataList.add(word);
            this.forward(this.dataList);
        }
        
    }
    
    public void close() throws HiveException {
    }
}

