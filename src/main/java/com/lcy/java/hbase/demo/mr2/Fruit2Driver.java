package com.lcy.java.hbase.demo.mr2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Fruit2Driver implements Tool {
    private Configuration configuration = null;
    
    public Fruit2Driver() {
    }
    
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(this.configuration);
        job.setJarByClass(Fruit2Driver.class);
        TableMapReduceUtil.initTableMapperJob("fruit", new Scan(), Fruit2Mapper.class, ImmutableBytesWritable.class, Put.class, job);
        TableMapReduceUtil.initTableReducerJob("fruit2", Fruit2Reducer.class, job);
        boolean result = job.waitForCompletion(true);
        return result ? 0 : 1;
    }
    
    public void setConf(Configuration conf) {
        this.configuration = conf;
    }
    
    public Configuration getConf() {
        return this.configuration;
    }
    
    public static void main(String[] args) {
        try {
            Configuration configuration = HBaseConfiguration.create();
            ToolRunner.run(configuration, new Fruit2Driver(), args);
        } catch (Exception var2) {
            var2.printStackTrace();
        }
        
    }
}
