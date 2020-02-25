package com.lcy.java.hbase.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
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

public class Txt2FruitRunner extends Configured implements Tool {
    
    public static void main(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        int status = ToolRunner.run(conf, new Txt2FruitRunner(), args);
        System.exit(status);
    }
    
    @Override
    public int run(String[] args) throws Exception {
        //得到Configuration
        Configuration conf = this.getConf();
        
        //创建Job任务
        Job job = Job.getInstance(conf, this.getClass().getSimpleName());
        job.setJarByClass(Txt2FruitRunner.class);
        Path inPath = new Path("hdfs://hadoop102:9000/input_fruit/fruit.tsv");
        FileInputFormat.addInputPath(job, inPath);
        
        //设置Mapper
        job.setMapperClass(ReadFruitFromHDFSMapper.class);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(Put.class);
        
        //设置Reducer
        TableMapReduceUtil.initTableReducerJob("fruit_mr",
                WriteFruitMRFromTxtReducer.class, job);
        
        //设置Reduce数量，最少1个
        job.setNumReduceTasks(1);
        
        boolean isSuccess = job.waitForCompletion(true);
        if (!isSuccess) {
            throw new IOException("Job running with error");
        }
        return isSuccess ? 0 : 1;
    }
    
    class ReadFruitFromHDFSMapper extends Mapper<LongWritable,
            Text, ImmutableBytesWritable, Put> {
        @Override
        protected void map(LongWritable key, Text value, Context
                context) throws IOException, InterruptedException {
            //从HDFS中读取的数据
            String lineValue = value.toString();
            //读取出来的每行数据使用\t进行分割，存于String数组
            String[] values = lineValue.split("\t");
            
            //根据数据中值的含义取值
            String rowKey = values[0];
            String name = values[1];
            String color = values[2];
            
            //初始化rowKey
            ImmutableBytesWritable rowKeyWritable = new
                    ImmutableBytesWritable(Bytes.toBytes(rowKey));
            
            //初始化put对象
            Put put = new Put(Bytes.toBytes(rowKey));
            
            //参数分别:列族、列、值
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes(name));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("color"), Bytes.toBytes(color));
            context.write(rowKeyWritable, put);
        }
    }
    
    class WriteFruitMRFromTxtReducer extends
            TableReducer<ImmutableBytesWritable, Put, NullWritable> {
        @Override
        protected void reduce(ImmutableBytesWritable key, Iterable<Put> values, Context context) throws IOException,
                InterruptedException {
            //读出来的每一行数据写入到fruit_hdfs表中
            for (Put put : values) {
                context.write(NullWritable.get(), put);
            }
        }
    }
    
}
