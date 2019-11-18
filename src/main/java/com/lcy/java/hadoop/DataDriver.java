package com.lcy.java.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;

/**
 * 把Mapper 和 Reducer  组装起来 或者 组合起来
 * <p>
 * 测试这个代码 就是 把这个打成一个Jar包  运行在集群jar上面
 * 也可以采用本地运行的模式
 */
public class DataDriver implements Tool {
    
    private Configuration configuration = null;
    
    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure(); //自动快速地使用缺省Log4j环境。
        Configuration configuration = new Configuration();
        int run = ToolRunner.run(configuration, new DataDriver(), args);
        System.exit(run);
    }
    
    @Override
    public int run(String[] args) throws Exception {
    
        //1.	或者job信息
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);
    
        //2.	获取jar包位置
        job.setJarByClass(DataDriver.class);
    
        //3.	关联自定义的mapper和reducer
        job.setMapperClass(DataMapper.class);
        job.setReducerClass(DataReducer.class);
    
        //4.	设置map输出数据类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
    
        //5.	设置最终输出数据类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
    
        //6.	设置数据输入和输出文件路径
        FileInputFormat.setInputPaths(job, new Path("file:\\D:\\sparkData\\data2.txt"));
        FileOutputFormat.setOutputPath(job, new Path("file:\\D:\\sparkData\\result"));
    
        //7.	提交代码
         job.submit(); //提交过后完事了
        //	下面这个是等待结束0
        boolean result = job.waitForCompletion(true);
        
        return result ? 0 : 1;
    }
    
    @Override
    public void setConf(Configuration conf) {
        this.configuration = conf;
    }
    
    @Override
    public Configuration getConf() {
        return configuration;
    }
}


/**
 * 输入的key  LongWritable  行号
 * 输入的value	Text	一行内容
 * 输出的key	Text		单词
 * 输出的value	IntWritable	单词的个数
 * <p>
 * Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>  前两个是输入的  后两个是输出的
 */
class DataMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    Text k = new Text();
    IntWritable v = new IntWritable(1);
    
    //	hello world
    //	hadoop
    //	spark
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        //1.	一行内容转换成String
        String line = value.toString();
        //2.	切割
        String[] words = line.split(" ");
        
        //3.	循环写出到下一个阶段
        for (String word : words) {
            //这是输出的 K 和 V
            //因为输出的时候  K 是Text 类型  V 是 Int类型
            //context.write(new Text(word), new IntWritable(1));
            //这样的话  太耗费资源了 每次都要new
            //所以在外面我们直接创建一个Text k  IntWritable v
            k.set(word);
            context.write(k, v);
        }
    }
}


/*
 *	前两个是输入   后两个是输出
 *	Reducer 的输入 就是 Mapper的输出
 */
class DataReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    //	hello world
    //	hello world
    
    //	hello 1
    //	hello 1
    //	world 1
    //	world 1  等等····
    //把相同的key（比如hello） 都传到key里面去    把这些1都传进去 进行迭代   输出靠text
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values,
                          Context context) throws IOException, InterruptedException {
        //1.	统计单词总个数   那么就要遍历values
        int sum = 0;   //进行累加
        for (IntWritable count : values) {
            sum += count.get();
        }
        //2.	输出单词总个数
        context.write(key, new IntWritable(sum));
        
    }
    
}
