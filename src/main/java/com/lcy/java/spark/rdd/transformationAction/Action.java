package com.lcy.java.spark.rdd.transformationAction;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.After;
import org.junit.Before;

public class Action {


    SparkConf conf = null;
    JavaSparkContext sc = null;

    @Before
    public void before(){
        conf = new SparkConf().setAppName("Action").setMaster("local");
        sc = new JavaSparkContext(conf);
    }

//    /**
//     * reduce（function）
//     * reduce其实是将RDD中的所有元素进行合并，当运行call方法时，会传入两个参数，
//     * 在call方法中将两个参数合并后返回，而这个返回值回合一个新的RDD中的元素再次传入call方法中，
//     * 继续合并，直到合并到只剩下一个元素时。
//     */
//
//    @Test
//    public void reduce(){
//        JavaRDD<Integer> parallelize = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7));
//        Integer reduce = parallelize.reduce((v1, v2) -> (v1 + v2));
//        System.out.println(reduce);
//    }
//
//    /**
//     * count（）
//     * 将一个RDD以一个Array数组形式返回其中的所有元素。
//     */
//    @Test
//    public void collect(){
//        JavaRDD<Integer> parallelize = sc.parallelize(Arrays.asList(1, 3, 5, 7, 9),2);
//        List<Integer> collect = parallelize.collect();
//        collect.forEach(e -> System.out.println(e));
//    }
//
//    /**
//     * count（）
//     * 返回数据集中元素个数，默认Long类型。
//     */
//    @Test
//    public void count(){
//        long count1 = sc.parallelize(Arrays.asList(1, 3, 5, 7, 9)).count();
//        System.out.println(count1);
//    }
//
//    /**
//     * first（）
//     * 返回数据集的第一个元素（类似于take(1)）
//     */
//    @Test
//    public void first(){
//        JavaRDD<Integer> parallelize = sc.parallelize(Arrays.asList(123, 3, 5, 7, 9), 2);
//        Integer first = parallelize.first();
//        System.out.println(first);
//    }
//
//    /**
//     * takeSample（withReplacement， num， [seed]）
//     * 对于一个数据集进行随机抽样，返回一个包含num个随机抽样元素的数组，withReplacement表示
//     * 是否有放回抽样，参数seed指定生成随机数的种子。
//     * 该方法仅在预期结果数组很小的情况下使用，因为所有数据都被加载到driver端的内存中。
//     */
//    @Test
//    public void takeSample(){
//        JavaRDD<Integer> parallelize = sc.parallelize(Arrays.asList(11, 23, 35, 47, 29));
//        List<Integer> integers = parallelize.takeSample(true, 3, 1);
//        integers.forEach(e -> System.out.println(e));
//    }
//
//    /**
//     * take（n）
//     * 返回一个包含数据集前n个元素的数组（从0下标到n-1下标的元素），不排序。
//     */
//    @Test
//    public void take(){
//        JavaRDD<Integer> parallelize = sc.parallelize(Arrays.asList(2, 7, 1, 8, 3), 2);
//        List<Integer> take = parallelize.take(3);
//        take.forEach(e -> System.out.println(e));
//    }
//
//    /**
//     * takeOrdered（n，[ordering]）
//     * 返回RDD中前n个元素，并按默认顺序排序（升序）或者按自定义比较器顺序排序。
//     */
//    @Test
//    public void takeOrdered(){
//        JavaRDD<Integer> parallelize = sc.parallelize(Arrays.asList(2, 7, 1, 8, 3), 2);
//        List<Integer> integers = parallelize.takeOrdered(3);
//        integers.forEach(e -> System.out.println(e));
//    }
//
//    /**
//     * saveAsTextFile（path）
//     * 将dataSet中元素以文本文件的形式写入本地文件系统或者HDFS等。Spark将对每个元素调用toString方法，
//     * 将数据元素转换为文本文件中的一行记录。
//     * 若将文件保存到本地文件系统，那么只会保存在executor所在机器的本地目录。
//     */
//    @Test
//    public void saveAsTextFile(){
//        JavaRDD<Integer> parallelize = sc.parallelize(Arrays.asList(2, 7, 1, 8, 3), 1);
//        JavaRDD<Integer> map = parallelize.map(v -> v + 2);
//        map.saveAsTextFile("file:\\D:\\sparkData\\data\\");
//    }
//
//    /**
//     * saveAsSequenceFile（path）（Java and Scala）
//     * 将dataSet中元素以Hadoop SequenceFile的形式写入本地文件系统或者HDFS等。（对pairRDD操作）
//     */
//    @Test
//    public void saveAsSequenceFile(){
//        JavaRDD<Integer> parallelize = sc.parallelize(Arrays.asList(2, 7, 1, 8, 3), 1);
//        JavaRDD<Integer> map = parallelize.map(v -> v + 2);
//        map.saveAsObjectFile("file:\\D:\\sparkData\\data\\");
//    }
//
//    /**
//     * countByKey（）
//     * 用于统计RDD[K,V]中每个K的数量，返回具有每个key的计数的（k，int）pairs的hashMap。
//     */
//    @Test
//    public void countByKey(){
//        JavaRDD<String> stringJavaRDD = sc.textFile("file:\\D:\\sparkData\\data2.txt", 2);
//        JavaRDD<String> stringJavaRDD1 = stringJavaRDD.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
//        JavaPairRDD<String, Integer> stringIntegerJavaPairRDD = stringJavaRDD1.mapToPair(e -> new Tuple2<>(e, 1));
//        JavaPairRDD<String, Integer> stringIntegerJavaPairRDD1 = stringIntegerJavaPairRDD.reduceByKey(((v1, v2) -> v1 + v2));
//        stringIntegerJavaPairRDD1.foreach(e -> System.out.println(e._1 + " " + e._2));
//    }
//
//    /**
//     * foreach（function）
//     */
//    @Test
//    public void foreach(){
//
//    }

    @After
    public void after(){
        sc.close();
    }
}
