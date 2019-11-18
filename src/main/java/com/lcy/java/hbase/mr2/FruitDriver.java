package com.lcy.java.hbase.mr2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

// 将 fruit 表中的一部分数据，通过 MR 迁入到 fruit_mr 表中。

public class FruitDriver extends Configured implements Tool {
    
    public static void main(String[] args) throws Exception {
        //本地测试configuration获取方法
        Configuration configuration = HBaseConfiguration.create();
        int run = ToolRunner.run(configuration, new FruitDriver(), args);
        System.exit(run);
    }
    
    @Override
    public int run(String[] args) throws Exception {
        //获取job对象
        Job job = Job.getInstance(this.getConf());
        //设置类路径
        job.setJarByClass(FruitDriver.class);
        //设置Mapper 输出K/V类型
        TableMapReduceUtil.initTableMapperJob(
                "fruit", //要读取的表
                new Scan(), //没有参数代表全表扫描
                FruitMapper.class, //Mapper类
                ImmutableBytesWritable.class, //输出Key
                Put.class, //输出Value
                job
        );
        //设置Reducer 不需要指定输出类型
        TableMapReduceUtil.initTableReducerJob(
                "fruit2", //输出表名 需要提前创建好
                FruitReduce.class, //Reducer类
                job
        );
        boolean b = job.waitForCompletion(true);
        return b ? 0 : 1;
    }
    
    class FruitMapper extends TableMapper<ImmutableBytesWritable, Put> {
        @Override
        protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException,
                InterruptedException {
            Put put = new Put(key.get());
            for (Cell cell : value.rawCells()) {
                if ("name".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))) {
                    put.add(cell);
                }
            }
            context.write(key, put);
        }
    }
    
    class FruitReduce extends TableReducer<ImmutableBytesWritable, Put, NullWritable> {
        @Override
        protected void reduce(ImmutableBytesWritable key, Iterable<Put> values, Context context)
                throws IOException, InterruptedException {
            for (Put put : values) {
                context.write(NullWritable.get(), put);
            }
        }
    }
    
}
