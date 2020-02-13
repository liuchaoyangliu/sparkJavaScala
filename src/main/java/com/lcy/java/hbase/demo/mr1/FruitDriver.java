package com.lcy.java.hbase.demo.mr1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class FruitDriver implements Tool {
    private Configuration configuration = null;
    
    public FruitDriver() {
    }
    
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(this.configuration);
        job.setJarByClass(FruitDriver.class);
        job.setMapperClass(FruitMapper.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);
        TableMapReduceUtil.initTableReducerJob(args[1], FruitReducer.class, job);
        FileInputFormat.setInputPaths(job, new Path[]{new Path(args[0])});
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
            Configuration configuration = new Configuration();
            int run = ToolRunner.run(configuration, new FruitDriver(), args);
            System.exit(run);
        } catch (Exception var3) {
            var3.printStackTrace();
        }
        
    }
}

