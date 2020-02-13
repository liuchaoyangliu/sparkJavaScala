package com.lcy.java.hbase.demo.mr2;

import java.io.IOException;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Mapper;

public class Fruit2Mapper extends TableMapper<ImmutableBytesWritable, Put> {
    public Fruit2Mapper() {
    }
    
    protected void map(ImmutableBytesWritable key, Result value, Mapper<ImmutableBytesWritable, Result, ImmutableBytesWritable, Put>.Context context) throws IOException, InterruptedException {
        Put put = new Put(key.get());
        Cell[] var5 = value.rawCells();
        int var6 = var5.length;
        
        for(int var7 = 0; var7 < var6; ++var7) {
            Cell cell = var5[var7];
            if ("name".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))) {
                put.add(cell);
            }
        }
        
        context.write(key, put);
    }
}

