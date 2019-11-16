package com.lcy.java.hbase.mr2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class FruitDriver implements Tool {

    private Configuration conf = null;

    public static void main(String[] args) throws Exception {

        // 导入hbase-site.xml文件后可直接本地运行，不需要jar包后扔 yarn 运行
        Configuration configuration = HBaseConfiguration.create();
        int run = ToolRunner.run(configuration, new FruitDriver(), args);

        System.exit(run);
    }


    @Override
    public int run(String[] args) throws Exception {

        // 1 获取Job对象
        Job job = Job.getInstance(conf);

        // 2 设置类路径
        job.setJarByClass(FruitDriver.class);

        // 3 设置Mapper
        TableMapReduceUtil.initTableMapperJob("fruit1", new Scan(),
                FruitMapper.class, ImmutableBytesWritable.class, Put.class, job);

        // 4 设置Reducer(不需要指定输出类型)
        TableMapReduceUtil.initTableReducerJob("fruit2", FruitReducer.class, job);

        // 5. 提交任务
        boolean b = job.waitForCompletion(true);
        System.exit(b  ? 0 : 1);

        return 0;
    }

    @Override
    public void setConf(Configuration configuration) {

        conf = configuration;
    }

    @Override
    public Configuration getConf() {
        return conf;
    }
    
}
