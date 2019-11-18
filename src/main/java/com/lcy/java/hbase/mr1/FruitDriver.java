package com.lcy.java.hbase.mr1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

// 将 HDFS 中的数据写入到 Hbase 表中。

public class FruitDriver implements Tool {
    
    private Configuration configuration = null;
    
    public static void main(String[] args) throws Exception {
        //打包运行
        Configuration configuration = new Configuration();
        int run = ToolRunner.run(configuration, new FruitDriver(), args);
        System.exit(run);
    }
    
    @Override
    public int run(String[] strings) throws Exception {
        //获取job对象
        Job job = Job.getInstance(configuration);
        //设置驱动类路径
        job.setJarByClass(FruitDriver.class);
        //设置Mapper  Mapper输出的类型
        job.setMapperClass(FruitMapper.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);
        //设置Reduce类，
        //第一个传输是表名， 表需要提前创建好
        TableMapReduceUtil.initTableReducerJob(strings[1], FruitReduce.class, job);
        //设置输入路径
        FileInputFormat.setInputPaths(job, new Path(strings[0]));
        //提交任务
        boolean b = job.waitForCompletion(true);
        
        return b ? 0 :1;
    }
    
    @Override
    public void setConf(Configuration configuration) {
        this.configuration = configuration;
    }
    
    @Override
    public Configuration getConf() {
        return configuration;
    }
    
    
    class FruitMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
    
        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            context.write(key, value);
        }
    }
    
    class FruitReduce extends TableReducer<LongWritable, Text, NullWritable>{
        @Override
        protected void reduce(LongWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            //遍历values ：1001    apple   red
            for (Text value : values) {
                //获取每一行数据
                String[] fields = value.toString().split("\t");
                //构建put对象
                Put put = new Put(Bytes.toBytes(fields[0]));
                put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes(fields[1]));
                put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("color"), Bytes.toBytes(fields[2]));
                //输出value 是put对象
                context.write(NullWritable.get(), put);
            }
        }
    }
    
}

/*
# 1 建表
hbase(main):001:0> create 'fruit1', 'info'
# 2 上传文件到HDFS
hdfs dfs -put /Users/zxy/IdeaProjects/bigdata-learning/hbase-learning/data/input/fruit.tsv /
# 3 执行命令，注意这里是hadoop里的yarn
/usr/local/hadoop-2.7.7/bin/yarn jar /Users/zxy/IdeaProjects/bigdata-learning/hbase-learning/target/hbase-learning-1
.0-SNAPSHOT.jar com.zouxxyy.hbase.mr1.FruitDriver /fruit.tsv fruit1
 */
