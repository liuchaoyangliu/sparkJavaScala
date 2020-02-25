package com.lcy.java.hbase.mr;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Fruit2FruitMRRunner extends Configured implements Tool {
    
    public static void main(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        int status = ToolRunner.run(conf, new Fruit2FruitMRRunner(), args);
        System.exit(status);
    }
    
    //组装Job
    public int run(String[] args) throws Exception {
        //得到Configuration
        Configuration conf = this.getConf();
        //创建Job任务
        Job job = Job.getInstance(conf, this.getClass().getSimpleName());
        job.setJarByClass(Fruit2FruitMRRunner.class);
        
        //配置Job
        Scan scan = new Scan();
        scan.setCacheBlocks(false);
        scan.setCaching(500);
        
        //设置Mapper，注意导入的是mapreduce包下的，不是mapred包下的，后者是老版本
        TableMapReduceUtil.initTableMapperJob(
                "fruit", //数据源的表名
                scan, //scan扫描控制器
                ReadFruitMapper.class,//设置Mapper类
                ImmutableBytesWritable.class,//设置Mapper输出key类型
                Put.class,//设置Mapper输出value值类型
                job//设置给哪个JOB
        );
        //设置Reducer
        TableMapReduceUtil.initTableReducerJob("fruit_mr",
                WriteFruitMRReducer.class, job);
        //设置Reduce数量，最少1个
        job.setNumReduceTasks(1);
        
        boolean isSuccess = job.waitForCompletion(true);
        if (!isSuccess) {
            throw new IOException("Job running with error");
        }
        return isSuccess ? 0 : 1;
    }
    
    
    class ReadFruitMapper extends
            TableMapper<ImmutableBytesWritable, Put> {
        
        @Override
        protected void map(ImmutableBytesWritable key, Result value, Context context)
                throws IOException, InterruptedException {
            //将fruit的name和color提取出来，相当于将每一行数据读取出来放入到Put 对象中。
            Put put = new Put(key.get());
            //遍历添加column行
            for (Cell cell : value.rawCells()) {
                //添加/克隆列族:info
                
                if ("info".equals(Bytes.toString(CellUtil.cloneFamily(cell)))) {
                    //添加/克隆列：name
                    
                    if ("name".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))) {
                        //将该列cell加入到put对象中      put.add(cell);
                        //添加/克隆列:color
                    } else if ("color".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))) {
                        //向该列cell加入到put对象中      put.add(cell);
                    }
                }
            }
            //将从fruit读取到的每行数据写入到context中作为map的输出
            context.write(key, put);
        }
    }
    
    
    class WriteFruitMRReducer extends
            TableReducer<ImmutableBytesWritable, Put, NullWritable> {
        @Override
        protected void reduce(ImmutableBytesWritable key, Iterable<Put> values, Context context) throws IOException,
                InterruptedException {
            //读出来的每一行数据写入到fruit_mr表中
            for (Put put : values) {
                context.write(NullWritable.get(), put);
            }
        }
    }
    
    
}

