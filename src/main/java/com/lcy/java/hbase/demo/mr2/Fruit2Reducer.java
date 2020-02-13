package com.lcy.java.hbase.demo.mr2;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class Fruit2Reducer extends TableReducer<ImmutableBytesWritable, Put, NullWritable> {
    public Fruit2Reducer() {
    }
    
    protected void reduce(ImmutableBytesWritable key, Iterable<Put> values, Reducer<ImmutableBytesWritable, Put, NullWritable, Mutation>.Context context) throws IOException, InterruptedException {
        Iterator var4 = values.iterator();
        
        while(var4.hasNext()) {
            Put put = (Put)var4.next();
            context.write(NullWritable.get(), put);
        }
        
    }
}

