package com.lcy.java.spark.rdd.transformationAction;

import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import scala.Tuple2;
import scala.Tuple3;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class TransFormation {

    SparkConf conf = null;
    JavaSparkContext jsc = null;
    @Before
    public void before(){
        conf = new SparkConf().setAppName("sparkStudy").setMaster("local");
        jsc = new JavaSparkContext(conf);
        jsc.setLogLevel("ERROR");
    }

    @Test
    public void map(){
        JavaRDD<Integer> parallelize = jsc.parallelize(Arrays.asList(1, 4, 7, 10));
        JavaRDD<Integer> map = parallelize.map(e -> (e + 1));
        map.foreach(e -> System.out.println(e));
    }

    /**
     * flatMap（function）
     * map是对RDD中元素逐一进行函数操作映射为另外一个RDD，而flatMap操作是将函数应用于RDD之中
     * 的每一个元素，将返回的迭代器的所有内容构成新的RDD。而flatMap操作是将函数应用于RDD中每一
     * 个元素，将返回的迭代器的所有内容构成RDD。
     *
     * flatMap与map区别在于map为“映射”，而flatMap“先映射，后扁平化”，map对每一次（func）都产生
     * 一个元素，返回一个对象，而flatMap多一步就是将所有对象合并为一个对象。
     */
    @Test
    public void flatMap(){
        List<String> list = Arrays.asList("张无忌 赵敏","宋青书 周芷若");
        JavaRDD<String> listRDD = jsc.parallelize(list);
        listRDD.flatMap(line -> Arrays.asList(line.split(" ")).iterator())
                .map(name -> "Hello " + name)
                .foreach(s -> System.out.println(s));

    }

    /**
     * filter（function）
     * 过滤操作，满足filter内function函数为true的RDD内所有元素组成一个新的数据集
     * 将一定的数据压缩到更少的partition分区中去
     * 使用场景！很多时候在filter算子应用之后会优化一下使用coalesce算子
     * filter 算子应用到RDD上面，说白了会应用到RDD对应的里面的每一个partition上去
     * 数据倾斜，换句话说就是可能有的partition里面就剩下了一条数据！
     * 建议用coalesce算子，从而让各个partition中的数据更加紧凑！！
     *
     * coalesce算子，
     */
    @Test
    public void filter(){
        JavaRDD<Integer> parallelize = jsc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));
        JavaRDD<Integer> filter = parallelize.filter(e -> e % 2 == 0);
        filter.foreach(e -> System.out.println(e));

    }


    /**
     * mapPartitions（function）
     * 区于foreachPartition（属于Action，且无返回值），而mapPartitions可获取返回值。
     * 与map的区别前面已经提到过了，但由于单独运行于RDD的每个分区上（block），
     * 所以在一个类型为T的RDD上运行时，（function）必须是
     * Iterator<T> => Iterator<U>类型的方法（入参）。
     */
    @Test
    public void mapPartitions(){
        List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6);
        JavaRDD<Integer> parallelize = jsc.parallelize(list, 2);
        JavaRDD<String> stringJavaRDD = parallelize.mapPartitions(integerIterator -> {
            ArrayList<String> array = new ArrayList<>();
            while (integerIterator.hasNext()) {
                array.add("hello" + integerIterator.next());
            }
            return array.iterator();
        });
        stringJavaRDD.foreach(e -> System.out.println(e));
    }

    /**
     * mapPartitionsWithIndex（function）
     * 与mapPartitions类似，但需要提供一个表示分区索引值的整型值作为参数，因此function必
     * 须是（int， Iterator<T>）=>Iterator<U>类型的。
     */
    @Test
    public void mapPartitionsWithIndex(){
        List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8);

        JavaRDD<Integer> parallelize = jsc.parallelize(list, 2);
        JavaRDD<String> stringJavaRDD = parallelize.mapPartitionsWithIndex((index, iterator) -> {
                    ArrayList<String> array = new ArrayList<>();
                    while (iterator.hasNext()) {
                        array.add(index + "_" + iterator.next());
                    }
                    return array.iterator();
                }, true
        );
        stringJavaRDD.foreach(e -> System.out.println(e));


    }


    /**
     * sample（withReplacement， fraction， seed）
     * withReplacement是否放回，fraction采样比例，seed用于指定的随机数生成器的种子
     * 是否放回抽样分true和false，fraction取样比例为(0, 1]。seed种子为整型实数。
     */
    @Test
    public void sample(){
        List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9);
        JavaRDD<Integer> parallelize = jsc.parallelize(list, 3);
        JavaRDD<Integer> sample = parallelize.sample(false, 0.5, 1);
        sample.foreach(e -> System.out.println(e));

    }

    /**
     * union（otherDataSet）
     * 对于源数据集和其他数据集求并集，不去重
     */
    @Test
    public void union(){
        List<String> list1 = Arrays.asList("qwer", "tyui", "poiu", "asdf", "ghjk", "zxcv");
        List<String> list2 = Arrays.asList("vbnm", "fghj", "rtyu", "qwer");
        JavaRDD<String> parallelize = jsc.parallelize(list1, 2);
        JavaRDD<String> parallelize1 = jsc.parallelize(list2);
        JavaRDD<String> union = parallelize1.union(parallelize);
        union.foreach(e -> System.out.println(e));

//        union.distinct().foreach( e -> System.out.println(e));

    }

    /**
     * intersection（otherDataSet）
     * 对于源数据集和其他数据集求交集，并去重，且无序返回。
     * 会去重
     */
    @Test
    public void intersection(){
        List<String> list1 = Arrays.asList("赵敏", "张无忌" , "周芷若", "小昭", "谢逊");
        List<String> list2 = Arrays.asList("赵敏", "张无忌" , "灭绝师太", "圣火令", "倚天剑");
        JavaRDD<String> parallelize = jsc.parallelize(list1,1);
        JavaRDD<String> parallelize1 = jsc.parallelize(list2, 1);
        JavaRDD<String> intersection = parallelize1.intersection(parallelize);
        intersection.foreach(e -> System.out.println(e));
    }


    /**
     * distinct（[numTasks]）
     * 返回一个在源数据集去重之后的新数据集，即去重，并局部无序而整体有序返回。
     *
     * 源码
     * def distinct(): RDD[T] = withScope {
     * distinct(partitions.length)
     * }
     * 从源码中看到有Shuffle操作
     */
    @Test
    public void distinct(){
        List<Integer> list1 = Arrays.asList(1,3,5,7,12,34,1,67,12,3,5,12,12,12,7,7,7,7);
        JavaRDD<Integer> parallelize = jsc.parallelize(list1, 1);
        JavaRDD<Integer> distinct = parallelize.distinct();
        List<Integer> collect = distinct.collect();
        collect.forEach(e -> System.out.println(e));

    }

    /**
     * groupByKey([numTasks])
     * groupByKey是将PairRDD中拥有相同key值得元素归为一组
     */
    @Test
    public void groupByKey(){

        List<Tuple2<String,String>> list = Arrays.asList(
                new Tuple2("武当", "张三丰"),
                new Tuple2("峨眉", "灭绝师太"),
                new Tuple2("武当", "宋青书"),
                new Tuple2("峨眉", "周芷若")
        );
        JavaPairRDD<String, String> stringStringJavaPairRDD = jsc.parallelizePairs(list);
        JavaPairRDD<String, Iterable<String>> stringIterableJavaPairRDD = stringStringJavaPairRDD.groupByKey();
        stringIterableJavaPairRDD.foreach(e -> System.out.println(e));


    }

    /**
     * reduceByKey（function，[numTasks]）
     * reduceByKey仅将RDD中所有K,V对中K值相同的V进行合并。
     */
    @Test
    public void reeduceByKey(){
        List<Tuple2<String, Integer>> list = Arrays.asList(
                new Tuple2<>("武当", 99),
                new Tuple2<>("少林", 97),
                new Tuple2<>("武当", 89),
                new Tuple2<>("少林", 77)
        );
        JavaPairRDD<String, Integer> stringIntegerJavaPairRDD = jsc.parallelizePairs(list);
        JavaPairRDD<String, Integer> stringIntegerJavaPairRDD1 = stringIntegerJavaPairRDD.reduceByKey(((v1, v2) -> v1 + v2));
        stringIntegerJavaPairRDD1.foreach(e -> System.out.println(e));

    }

    /**
     * aggregateByKey（zeroValue）（seqOp， combOp， [numTasks]）
     * aggregateByKey函数对PairRDD中相同Key的值进行聚合操作，在聚合过程中同样使用了一个中立的初始值。
     * 和aggregate函数类似，aggregateByKey返回值的类型不需要和RDD中value的类型一致。因为aggregateByKey
     * 是对相同Key中的值进行聚合操作，所以aggregateByKey函数最终返回的类型还是Pair RDD，对应的结果是Key和
     * 聚合好的值；而aggregate函数直接是返回非RDD的结果，这点需要注意。在实现过程中，定义了三个aggregateByKey
     * 函数原型，但最终调用的aggregateByKey函数都一致。
     * 或者
     * 类似reduceByKey，对pairRDD中想用的key值进行聚合操作，使用初始值（seqOp中使用，而combOpenCL中未使用）
     * 对应返回值为pairRDD，而区于aggregate（返回值为非RDD）
     */
    @Test
    public void aggregateByKey(){
        List<String> list1 = Arrays.asList("hello world!", "I am a dog", "hello world!", "I am a dog");
        JavaRDD<String> parallelize = jsc.parallelize(list1);
        JavaRDD<String> stringJavaRDD = parallelize.flatMap(e -> Arrays.asList(e.split(" ")).iterator());
        JavaPairRDD<String, Integer> stringIntegerJavaPairRDD = stringJavaRDD.mapToPair(e -> new Tuple2<>(e, 1));
        JavaPairRDD<String, Integer> stringIntegerJavaPairRDD1 = stringIntegerJavaPairRDD.aggregateByKey(
                0,
                (i1, i2) -> i1 + i2,
                (i1, i2) -> i1 + i2);
        stringIntegerJavaPairRDD1.foreach(e -> System.out.println(e));

    }

    /**
     * sortByKey（[ascending], [numTasks]）
     *
     */
    @Test
    public void sortByKey(){
        List<Tuple2<String, Integer>> list = Arrays.asList(
                new Tuple2<>("武当", 99),
                new Tuple2<>("少林", 97),
                new Tuple2<>("武当", 89),
                new Tuple2<>("少林", 77)
        );

        JavaPairRDD<String, Integer> stringIntegerJavaPairRDD = jsc.parallelizePairs(list);
        JavaPairRDD<String, Integer> stringIntegerJavaPairRDD1 = stringIntegerJavaPairRDD.sortByKey(false);
        stringIntegerJavaPairRDD1.foreach(e -> System.out.println(e._1 + " " + e._2));


    }

    /**
     * join（otherDataSet，[numTasks]）
     * 加入一个RDD，在一个（k，v）和（k，w）类型的dataSet上调用，返回一个（k，（v，w））的pair dataSet。
     */
    @Test
    public void join(){
        final List<Tuple2<Integer, String>> names = Arrays.asList(
                new Tuple2<>(1, "东方不败"),
                new Tuple2<>(2, "令狐冲"),
                new Tuple2<>(3, "林平之")

        );
        final List<Tuple2<Integer, Integer>> scores = Arrays.asList(
                new Tuple2<>(1, 99),
                new Tuple2<>(2, 98),
                new Tuple2<>(3, 97)

        );

        final JavaPairRDD<Integer, String> integerStringJavaPairRDD = jsc.parallelizePairs(names);
        final JavaPairRDD<Integer, Integer> integerIntegerJavaPairRDD = jsc.parallelizePairs(scores);

        JavaPairRDD<Integer, Tuple2<Integer, String>> join =
                integerIntegerJavaPairRDD.join(integerStringJavaPairRDD);

        join.foreach(e -> System.out.println(e));

    }

    /**
     * cogroup（otherDataSet，[numTasks]）
     * 对两个RDD中的KV元素，每个RDD中相同key中的元素分别聚合成一个集合。与reduceByKey不同的是针对
     * 两个RDD中相同的key的元素进行合并。
     *
     * 合并两个RDD，生成一个新的RDD。实例中包含两个Iterable值，第一个表示RDD1中相同值，第二个表示RDD2
     * 中相同值（key值），这个操作需要通过partitioner进行重新分区，因此需要执行一次shuffle操作。（
     * 若两个RDD在此之前进行过shuffle，则不需要）
     */
    @Test
    public void cogroup(){
        List<Tuple2<Integer, String>> list1 = Arrays.asList(
                new Tuple2<>(1, "cat"),
                new Tuple2<>(2, "dog")
        );
        List<Tuple2<Integer, String>> list2 = Arrays.asList(
                new Tuple2<>(1, "tiger"),
                new Tuple2<>(1, "elephant"),
                new Tuple2<>(3, "panda"),
                new Tuple2<>(3, "chicken")

        );
        List<Tuple2<Integer, String>> list3 = Arrays.asList(
                new Tuple2<>(1, "duck"),
                new Tuple2<>(1, "lion"),
                new Tuple2<>(3, "bird"),
                new Tuple2<>(3, "fish"),
                new Tuple2<>(4, "flowers")
        );

        JavaPairRDD<Integer, String> integerStringJavaPairRDD = jsc.parallelizePairs(list1);
        JavaPairRDD<Integer, String> integerStringJavaPairRDD1 = jsc.parallelizePairs(list2);
        JavaPairRDD<Integer, String> integerStringJavaPairRDD2 = jsc.parallelizePairs(list3);

        JavaPairRDD<Integer, Tuple3<Iterable<String>, Iterable<String>, Iterable<String>>> cogroup =
                integerStringJavaPairRDD.cogroup(integerStringJavaPairRDD1, integerStringJavaPairRDD2);

        cogroup.foreach(e -> System.out.println(e));

    }

    /**
     * cartesian（otherDataSet）
     * 求笛卡尔乘积。该操作不会执行shuffle操作。
     */
    @Test
    public void cartesion(){
        List<String> strings = Arrays.asList("A", "B");
        List<Integer> integers = Arrays.asList(1, 2, 3);
        JavaRDD<String> parallelize = jsc.parallelize(strings);
        JavaRDD<Integer> parallelize1 = jsc.parallelize(integers);
        JavaPairRDD<String, Integer> cartesian = parallelize.cartesian(parallelize1);
        cartesian.foreach(e -> System.out.println(e));
    }

    @Test
    public void pipe(){

    }

    /**
     * coalesce（numPartitions）
     * 重新分区，减少RDD中分区的数量到numPartitions。
     *
     */
    @Test
    public void coalesce(){
        List<Integer> integers = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9);
        JavaRDD<Integer> parallelize = jsc.parallelize(integers,4);
        JavaRDD<Integer> coalesce = parallelize.coalesce(2);
        coalesce.foreach(e -> System.out.println(e));
    }

    /**
     * repartition（numPartitions）
     * repartition是coalesce接口中shuffle为true的简易实现，即Reshuffle RDD并随机分区，使各分区数据量
     * 尽可能平衡。若分区之后分区数远大于原分区数，则需要shuffle。
     */
    @Test
    public void repartition(){
        List<Integer> integers = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9);
        JavaRDD<Integer> parallelize = jsc.parallelize(integers,1);
        JavaRDD<Integer> repartition = parallelize.repartition(2);
        repartition.foreach(e -> System.out.println(e));

    }

    /**
     *.repartitionAndSortWithinPartitions（partitioner）
     * repartitionAndSortWithinPartitions函数是repartition函数的变种，与repartition函数不同的是，
     * repartitionAndSortWithinPartitions在给定的partitioner内部进行排序，性能比repartition要高。
     */
    //org.apache.spark.SparkException: Job aborted due to stage failure: Task not serializable: java.io.NotSerializableException:
    // com.lcy.java.demo.RDD.transformationAction.TransFormation
    //未序列化，但是未找出原因
    @Test
    public void repartitionAndSortWithinPartitions(){
        List<Integer> list = Arrays.asList(1, 3, 55, 77, 33, 5, 23);
        JavaRDD<Integer> parallelize = jsc.parallelize(list, 1);
        JavaPairRDD<Integer, Integer> integerIntegerJavaPairRDD = parallelize.mapToPair(e -> new Tuple2<>(e, e));
        JavaPairRDD<Integer, Integer> integerIntegerJavaPairRDD1 =
                integerIntegerJavaPairRDD.repartitionAndSortWithinPartitions(new Partitioner() {
            @Override
            public int getPartition(Object key) {
                Integer index = Integer.valueOf(key.toString());
                if (index % 2 == 0) {
                    return 0;
                } else {
                    return 1;
                }
            }

            @Override
            public int numPartitions() {
                return 2;
            }
        });
        JavaRDD<String> stringJavaRDD = integerIntegerJavaPairRDD1.mapPartitionsWithIndex((index, iterator) -> {
            final ArrayList<String> strings = new ArrayList<>();
            while (iterator.hasNext()) {
                strings.add(index + "_" + iterator.next());
            }
            return strings.iterator();
        }, false);
        stringJavaRDD.foreach(e -> System.out.println(e));

    }



    /**
     * reduce（function）
     * reduce其实是将RDD中的所有元素进行合并，当运行call方法时，会传入两个参数，
     * 在call方法中将两个参数合并后返回，而这个返回值回合一个新的RDD中的元素再次传入call方法中，
     * 继续合并，直到合并到只剩下一个元素时。
     */

    @Test
    public void reduce(){
        JavaRDD<Integer> parallelize = jsc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7));
        Integer reduce = parallelize.reduce((v1, v2) -> (v1 + v2));

        System.out.println(reduce);
    }

    /**
     * count（）
     * 将一个RDD以一个Array数组形式返回其中的所有元素。
     */
    @Test
    public void collect(){
        JavaRDD<Integer> parallelize = jsc.parallelize(Arrays.asList(1, 3, 5, 7, 9),2);
        List<Integer> collect = parallelize.collect();
        collect.forEach(e -> System.out.println(e));
    }

    /**
     * count（）
     * 返回数据集中元素个数，默认Long类型。
     */
    @Test
    public void count(){
        long count1 = jsc.parallelize(Arrays.asList(1, 3, 5, 7, 9)).count();
        System.out.println(count1);
    }

    /**
     * first（）
     * 返回数据集的第一个元素（类似于take(1)）
     */
    @Test
    public void first(){
        JavaRDD<Integer> parallelize = jsc.parallelize(Arrays.asList(123, 3, 5, 7, 9), 2);
        Integer first = parallelize.first();
        System.out.println(first);
    }

    /**
     * takeSample（withReplacement， num， [seed]）
     * 对于一个数据集进行随机抽样，返回一个包含num个随机抽样元素的数组，withReplacement表示
     * 是否有放回抽样，参数seed指定生成随机数的种子。
     * 该方法仅在预期结果数组很小的情况下使用，因为所有数据都被加载到driver端的内存中。
     */
    @Test
    public void takeSample(){
        JavaRDD<Integer> parallelize = jsc.parallelize(Arrays.asList(11, 23, 35, 47, 29));
        List<Integer> integers = parallelize.takeSample(false, 3, 1);
        integers.forEach(e -> System.out.println(e));
    }

    /**
     * take（n）
     * 返回一个包含数据集前n个元素的数组（从0下标到n-1下标的元素），不排序。
     */
    @Test
    public void take(){
        JavaRDD<Integer> parallelize = jsc.parallelize(Arrays.asList(2, 7, 1, 8, 3), 2);
        List<Integer> take = parallelize.take(3);
        take.forEach(e -> System.out.println(e));
    }

    /**
     * takeOrdered（n，[ordering]）
     * 返回RDD中前n个元素，并按默认顺序排序（升序）或者按自定义比较器顺序排序。
     */
    @Test
    public void takeOrdered(){
        JavaRDD<Integer> parallelize = jsc.parallelize(Arrays.asList(2, 7, 1, 8, 3), 2);
        List<Integer> integers = parallelize.takeOrdered(3);
        integers.forEach(e -> System.out.println(e));
    }

    /**
     * saveAsTextFile（path）
     * 将dataSet中元素以文本文件的形式写入本地文件系统或者HDFS等。Spark将对每个元素调用toString方法，
     * 将数据元素转换为文本文件中的一行记录。
     * 若将文件保存到本地文件系统，那么只会保存在executor所在机器的本地目录。
     */
    @Test
    public void saveAsTextFile(){
        JavaRDD<Integer> parallelize = jsc.parallelize(Arrays.asList(2, 7, 1, 8, 3), 1);
        JavaRDD<Integer> map = parallelize.map(v -> v + 2);
        map.saveAsTextFile("file:\\D:\\sparkData\\data\\");
    }

    /**
     * saveAsSequenceFile（path）（Java and Scala）
     * 将dataSet中元素以Hadoop SequenceFile的形式写入本地文件系统或者HDFS等。（对pairRDD操作）
     */
    @Test
    public void saveAsSequenceFile(){
        JavaRDD<Integer> parallelize = jsc.parallelize(Arrays.asList(2, 7, 1, 8, 3), 1);
        JavaRDD<Integer> map = parallelize.map(v -> v + 2);
        map.saveAsObjectFile("file:\\D:\\sparkData\\data\\");
    }

    /**
     * countByKey（）
     * 用于统计RDD[K,V]中每个K的数量，返回具有每个key的计数的（k，int）pairs的hashMap。
     */
    @Test
    public void countByKey(){
        JavaRDD<String> stringJavaRDD =
                jsc.textFile("file:\\D:\\sparkData\\data2.txt", 2);

        JavaRDD<String> stringJavaRDD1 =
                stringJavaRDD.flatMap(line -> Arrays.asList(line.split(" ")).iterator());

        JavaPairRDD<String, Integer> stringIntegerJavaPairRDD =
                stringJavaRDD1.mapToPair(e -> new Tuple2<>(e, 1));

        JavaPairRDD<String, Integer> stringIntegerJavaPairRDD1 =
                stringIntegerJavaPairRDD.reduceByKey(((v1, v2) -> v1 + v2));

        stringIntegerJavaPairRDD1.foreach(e -> System.out.println(e._1 + " " + e._2));
    }


    @After
    public void after(){
        jsc.close();
    }

}
