package com.lcy.java.spark.rdd.transformationAction;

import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class TransFormation2 {

    static SparkConf conf = null;
    static JavaSparkContext sc = null;
    static {
        conf = new SparkConf();
        conf.setMaster("local").setAppName("SparkStudy");
        sc = new JavaSparkContext(conf);
    }

    public static void main(String[] args) {

//        map();
//        flatMap();
//        mapPartitions();
//        mapPartitionsWithIndex();
//        reduce();
//        reduceByKey();
//        union();
//        groupByKey();
//        join();
//        sample();
//        cartesian();
//        filter();
//        distinct();
//        intersection();
//        coalesce();
//        replication();
//        repartitionAndSortWithinPartitions();
//        cogroup();
//        sortByKey();
        aggregateByKey();

    }

    public static void map(){
        List<String> list = Arrays.asList("张无忌","赵敏","周芷若");
//        System.out.println(list.size());
//        JavaRDD<String> listRDD = sc.parallelize(list);
//
//        JavaRDD<String> nameRDD = listRDD.map(new Function<String, String>() {
//            @Override
//            public String call(String name) throws Exception {
//                return "Hello " + name;
//            }
//        });
//        nameRDD.foreach(new VoidFunction<String>() {
//            @Override
//            public void call(String s) throws Exception {
//                System.out.println(s);
//            }
//        });

        sc.parallelize(list)
                .map(name -> "Hello " + name)
                .foreach(s -> System.out.println(s));
        sc.stop();
    }


    public static void flatMap(){
        List<String> list = Arrays.asList("张无忌 赵敏","宋青书 周芷若");
//        JavaRDD<String> listRDD = sc.parallelize(list);
//
//        JavaRDD<String> nameRDD = listRDD
//                .flatMap(new FlatMapFunction<String, String>() {
//                    @Override
//                    public Iterator<String> call(String line) throws Exception {
//                        return Arrays.asList(line.split(" ")).iterator();
//                    }
//                })
//                .map(new Function<String, String>() {
//                    @Override
//                    public String call(String name) throws Exception {
//                        return "Hello " + name;
//                    }
//                });
//
//        nameRDD.foreach(new VoidFunction<String>() {
//            @Override
//            public void call(String s) throws Exception {
//                System.out.println(s);
//            }
//        });


        JavaRDD<String> listRDD = sc.parallelize(list);

        listRDD.flatMap(line -> Arrays.asList(line.split(" ")).iterator())
                .map(name -> "Hello " + name)
                .foreach(s -> System.out.println(s));
        sc.stop();
    }


    /**
     * map:
     *    一条数据一条数据的处理（文件系统，数据库等等）
     * mapPartitions：
     *    一次获取的是一个分区的数据（hdfs）
     *    正常情况下，mapPartitions 是一个高性能的算子
     *    因为每次处理的是一个分区的数据，减少了去获取数据的次数。
     *
     *    但是如果我们的分区如果设置得不合理，有可能导致每个分区里面的数据量过大。
     */
    public static void mapPartitions(){
        List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6);
        //参数二代表这个rdd里面有两个分区
//        JavaRDD<Integer> listRDD = sc.parallelize(list,2);
//
//        listRDD.mapPartitions(new FlatMapFunction<Iterator<Integer>, String>() {
//            @Override
//            public Iterator<String> call(Iterator<Integer> iterator) throws Exception {
//                ArrayList<String> array = new ArrayList<>();
//                while (iterator.hasNext()){
//                    array.add("hello " + iterator.next());
//                }
//                return array.iterator();
//            }
//        }).foreach(new VoidFunction<String>() {
//            @Override
//            public void call(String s) throws Exception {
//                System.out.println(s);
//            }
//        });


        sc.parallelize(list,2)
                .mapPartitions(iterator -> {
                    ArrayList<String> array = new ArrayList<>();
                    while (iterator.hasNext()){
                        array.add("hello " + iterator.next());
                    }
                    return array.iterator();
                }).foreach(s -> System.out.println(s));
        sc.stop();
    }
    /** 取和处理的就是一个分区的数据,并且知道处理的分区的分区号是啥？  */
    public static void mapPartitionsWithIndex(){
        List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8);
//        JavaRDD<Integer> listRDD = sc.parallelize(list, 2);
//        listRDD.mapPartitionsWithIndex(new Function2<Integer, Iterator<Integer>, Iterator<String>>() {
//            @Override
//            public Iterator<String> call(Integer index, Iterator<Integer> iterator) throws Exception {
//                ArrayList<String> list1 = new ArrayList<>();
//                while (iterator.hasNext()){
//                    list1.add(index+"_"+iterator.next());
//                }
//                return list1.iterator();
//            }
//        },true)
//                .foreach(new VoidFunction<String>() {
//                    @Override
//                    public void call(String s) throws Exception {
//                        System.out.println(s);
//                    }
//                });

        sc.parallelize(list, 2)
                .mapPartitionsWithIndex((index, iterator) -> {
            ArrayList<String> list1 = new ArrayList<>();
            while (iterator.hasNext()){
                list1.add(index+"_"+iterator.next());
            }
            return list1.iterator();
        },true)
                .foreach(s -> System.out.println(s));
        sc.stop();
    }

    public static void reduce(){
        List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6);
//        JavaRDD<Integer> listRDD = sc.parallelize(list);
//
//        Integer result = listRDD.reduce(new Function2<Integer, Integer, Integer>() {
//            @Override
//            public Integer call(Integer i1, Integer i2) throws Exception {
//                return i1 + i2;
//            }
//        });
//        System.out.println(result);

        JavaRDD<Integer> listRDD = sc.parallelize(list);

        Integer result = listRDD.reduce((v1, v2) -> v1 + v2);
        System.out.println(result);
    }

    public static void reduceByKey(){
//        List<Tuple2<String, Integer>> list = Arrays.asList(
//                new Tuple2<String, Integer>("武当", 99),
//                new Tuple2<String, Integer>("少林", 97),
//                new Tuple2<String, Integer>("武当", 89),
//                new Tuple2<String, Integer>("少林", 77)
//        );
//        JavaPairRDD<String, Integer> listRDD = sc.parallelizePairs(list);
//        //运行reduceByKey时，会将key值相同的组合在一起做call方法中的操作
//        JavaPairRDD<String, Integer> result = listRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
//            @Override
//            public Integer call(Integer i1, Integer i2) throws Exception {
//                return i1 + i2;
//            }
//        });
//        result.foreach(new VoidFunction<Tuple2<String, Integer>>() {
//            @Override
//            public void call(Tuple2<String, Integer> tuple) throws Exception {
//                System.out.println("门派: " + tuple._1 + "->" + tuple._2);
//            }
//        });



        List<Tuple2<String, Integer>> list = Arrays.asList(
                new Tuple2<>("武当", 99),
                new Tuple2<>("少林", 97),
                new Tuple2<>("武当", 89),
                new Tuple2<>("少林", 77)
        );
        sc.parallelizePairs(list)
                .reduceByKey((i1, i2) -> i1 + i2)
                .foreach(tuple -> System.out.println("门派: " + tuple._1 + "->" + tuple._2));

        sc.stop();
    }


    /** 当要将两个RDD合并时，便要用到union和join，其中union只是简单的将两个RDD累加起来，
     * 可以看做List的addAll方法。就想List中一样，当使用union及join时，必须保证两个RDD的泛型是一致的。
     */
    public static void union(){
//        final List<Integer> list1 = Arrays.asList(1, 2, 3, 4);
//        final List<Integer> list2 = Arrays.asList(3, 4, 5, 6);
//        final JavaRDD<Integer> rdd1 = sc.parallelize(list1);
//        final JavaRDD<Integer> rdd2 = sc.parallelize(list2);
//        rdd1.union(rdd2)
//                .foreach(new VoidFunction<Integer>() {
//                    @Override
//                    public void call(Integer number) throws Exception {
//                        System.out.println(number + "");
//                    }
//                });


        final JavaRDD<Integer> rdd1 = sc.parallelize(Arrays.asList(1, 2, 3, 4));
        final JavaRDD<Integer> rdd2 = sc.parallelize(Arrays.asList(3, 4, 5, 6));
        rdd1.union(rdd2)
                .foreach(number -> System.out.println(number));
        sc.stop();
    }

    public static void groupByKey(){
        List<Tuple2<String,String>> list = Arrays.asList(
                new Tuple2("武当", "张三丰"),
                new Tuple2("峨眉", "灭绝师太"),
                new Tuple2("武当", "宋青书"),
                new Tuple2("峨眉", "周芷若")
        );
//        JavaPairRDD<String, String> listRDD = sc.parallelizePairs(list);
//
//        JavaPairRDD<String, Iterable<String>> groupByKeyRDD = listRDD.groupByKey();
//        groupByKeyRDD.foreach(new VoidFunction<Tuple2<String, Iterable<String>>>() {
//            @Override
//            public void call(Tuple2<String, Iterable<String>> tuple) throws Exception {
//                String menpai = tuple._1;
//                Iterator<String> iterator = tuple._2.iterator();
//                String people = "";
//                while (iterator.hasNext()){
//                    people = people + iterator.next()+" ";
//                }
//                System.out.println("门派:"+menpai + "人员:"+people);
//            }
//        });


        sc.parallelizePairs(list)
                .groupByKey()
                .foreach(tuple ->System.out.println(tuple));
        sc.stop();
    }

    /** join是将两个PairRDD合并，并将有相同key的元素分为一组，可以理解为groupByKey和Union的结合 */
    public static void join(){
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

        final JavaPairRDD<Integer, String> nemesrdd = sc.parallelizePairs(names);
        final JavaPairRDD<Integer, Integer> scoresrdd = sc.parallelizePairs(scores);

//        final JavaPairRDD<Integer, Tuple2<String, Integer>> joinRDD = nemesrdd.join(scoresrdd);
//        joinRDD.foreach(new VoidFunction<Tuple2<Integer, Tuple2<String, Integer>>>() {
//            @Override
//            public void call(Tuple2<Integer, Tuple2<String, Integer>> tuple) throws Exception {
//                System.out.println("学号：" + tuple._1 + " 名字："+tuple._2._1 + " 分数："+tuple._2._2);
//            }
//        });

        nemesrdd.join(scoresrdd).foreach(value -> System.out.println(value));
        sc.stop();
    }

    public static void sample(){
        ArrayList<Integer> list = new ArrayList<>();
        for(int i=1;i<=100;i++){
            list.add(i);
        }
//        JavaRDD<Integer> listRDD = sc.parallelize(list);
//        /**
//         * sample用来从RDD中抽取样本。他有三个参数
//         * withReplacement: Boolean,
//         *       true: 有放回的抽样
//         *       false: 无放回抽象
//         * fraction: Double：
//         *      抽取样本的比例
//         * seed: Long：
//         *      随机种子
//         */
//        JavaRDD<Integer> sampleRDD = listRDD.sample(false, 0.1,0);
//        sampleRDD.foreach(new VoidFunction<Integer>() {
//            @Override
//            public void call(Integer num) throws Exception {
//                System.out.print(num+" ");
//            }
//        });


        sc.parallelize(list)
                .sample(false, 0.1,0)
                .foreach(num -> System.out.println(num));
        sc.stop();
    }

    /** cartesian是用于求笛卡尔积的 */
    public static void cartesian(){
        List<String> list1 = Arrays.asList("A", "B");
        List<Integer> list2 = Arrays.asList(1, 2, 3);
        JavaRDD<String> list1RDD = sc.parallelize(list1);
        JavaRDD<Integer> list2RDD = sc.parallelize(list2);
        list1RDD.cartesian(list2RDD).foreach(tuple -> System.out.println(tuple));
    }

    public static void filter(){
        List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
//        JavaRDD<Integer> listRDD = sc.parallelize(list);
//        JavaRDD<Integer> filterRDD = listRDD.filter(new Function<Integer, Boolean>() {
//            @Override
//            public Boolean call(Integer num) throws Exception {
//                return num % 2 == 0;
//            }
//        });
//        filterRDD.foreach(new VoidFunction<Integer>() {
//            @Override
//            public void call(Integer num) throws Exception {
//                System.out.print(num + " ");
//            }
//        });

        sc.parallelize(list)
                .filter(num -> num % 2 == 0)
                .foreach(num -> System.out.print(num + " "));
        sc.stop();

    }


    public static void distinct(){
        List<Integer> list = Arrays.asList(1, 1, 2, 2, 3, 3, 4, 5);
//        JavaRDD<Integer> listRDD  = (JavaRDD<Integer>) sc.parallelize(list);
//        JavaRDD<Integer> distinctRDD = listRDD.distinct();
//        distinctRDD.foreach(new VoidFunction<Integer>() {
//            @Override
//            public void call(Integer num) throws Exception {
//                System.out.println(num);
//            }
//        });

        sc.parallelize(list)
                .distinct()
                .foreach(num -> System.out.println(num));
        sc.stop();
    }

    public static void intersection(){
        List<Integer> list1 = Arrays.asList(1, 2, 3, 4);
        List<Integer> list2 = Arrays.asList(3, 4, 5, 6);
        JavaRDD<Integer> list1RDD = sc.parallelize(list1);
        JavaRDD<Integer> list2RDD = sc.parallelize(list2);
//        list1RDD.intersection(list2RDD).foreach(new VoidFunction<Integer>() {
//            @Override
//            public void call(Integer num) throws Exception {
//                System.out.println(num);
//            }
//        });

        list1RDD.intersection(list2RDD)
                .foreach(num -> System.out.println(num));
        sc.stop();

    }

    /** 分区数由多  -》 变少 */
    public static void coalesce(){
        List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9);
//        JavaRDD<Integer> listRDD = sc.parallelize(list, 3);
//        listRDD.coalesce(1).foreach(new VoidFunction<Integer>() {
//            @Override
//            public void call(Integer num) throws Exception {
//                System.out.print(num);
//            }
//        });

        sc.parallelize(list, 3)
                .coalesce(1)
                .foreach(num -> System.out.print(num));
        sc.stop();
    }

    /**  进行重分区，解决的问题：本来分区数少  -》 增加分区数 */
    public static void replication(){
        List<Integer> list = Arrays.asList(1, 2, 3, 4);
//        JavaRDD<Integer> listRDD = sc.parallelize(list, 1);
//        listRDD.repartition(2).foreach(new VoidFunction<Integer>() {
//            @Override
//            public void call(Integer num) throws Exception {
//                System.out.println(num);
//            }
//        });

        sc.parallelize(list, 1)
                .repartition(2).foreach(num -> System.out.println(num));
        sc.stop();
    }

    /**
     *repartitionAndSortWithinPartitions函数是repartition函数的变种，与repartition函数不同的是，
     * repartitionAndSortWithinPartitions在给定的partitioner内部进行排序，性能比repartition要高。
     *
     */
    public static void repartitionAndSortWithinPartitions(){
        List<Integer> list = Arrays.asList(1, 3, 55, 77, 33, 5, 23);
//        JavaRDD<Integer> listRDD = sc.parallelize(list, 1);
//        JavaPairRDD<Integer, Integer> pairRDD = listRDD.mapToPair(new PairFunction<Integer, Integer, Integer>() {
//            @Override
//            public Tuple2<Integer, Integer> call(Integer num) throws Exception {
//                return new Tuple2<>(num, num);
//            }
//        });
//        JavaPairRDD<Integer, Integer> parationRDD = pairRDD.repartitionAndSortWithinPartitions(new Partitioner() {
//            @Override
//            public int getPartition(Object key) {
//                Integer index = Integer.valueOf(key.toString());
//                if (index % 2 == 0) {
//                    return 0;
//                } else {
//                    return 1;
//                }
//
//            }
//
//            @Override
//            public int numPartitions() {
//                return 2;
//            }
//        });
//        parationRDD.mapPartitionsWithIndex(new Function2<Integer, Iterator<Tuple2<Integer, Integer>>, Iterator<String>>() {
//            @Override
//            public  Iterator<String> call(Integer index, Iterator<Tuple2<Integer, Integer>> iterator) throws Exception {
//                final ArrayList<String> list1 = new ArrayList<>();
//                while (iterator.hasNext()){
//                    list1.add(index+"_"+iterator.next());
//                }
//                return list1.iterator();
//            }
//        },false).foreach(new VoidFunction<String>() {
//            @Override
//            public void call(String s) throws Exception {
//                System.out.println(s);
//            }
//        });

        sc.parallelize(list, 1)
                .mapToPair( num -> new Tuple2<>(num, num))
                .repartitionAndSortWithinPartitions(new Partitioner() {
            public int getPartition(Object key) {
                Integer index = Integer.valueOf(key.toString());
                if (index % 2 == 0) {
                    return 0;
                } else {
                    return 1;
                }

            }
            public int numPartitions() {
                return 2;
            }
        })
                .mapPartitionsWithIndex((index, iterator) -> {
            final ArrayList<String> list1 = new ArrayList<>();
            while (iterator.hasNext()){
                list1.add(index+"_"+iterator.next());
            }
            return list1.iterator();
        },false).foreach(s -> System.out.println(s));

    }

    /** 对两个RDD中的KV元素，每个RDD中相同key中的元素分别聚合成一个集合。
     * 与reduceByKey不同的是针对两个RDD中相同的key的元素进行合并。
     */
    public static void cogroup(){
        List<Tuple2<Integer, String>> list1 = Arrays.asList(
                new Tuple2<>(1, "www"),
                new Tuple2<>(2, "bbs")
        );

        List<Tuple2<Integer, String>> list2 = Arrays.asList(
                new Tuple2<>(1, "cnblog"),
                new Tuple2<>(2, "cnblog"),
                new Tuple2<>(3, "very")
        );

        List<Tuple2<Integer, String>> list3 = Arrays.asList(
                new Tuple2<>(1, "com"),
                new Tuple2<>(2, "com"),
                new Tuple2<>(3, "good")
        );

        JavaPairRDD<Integer, String> list1RDD = sc.parallelizePairs(list1);
        JavaPairRDD<Integer, String> list2RDD = sc.parallelizePairs(list2);
        JavaPairRDD<Integer, String> list3RDD = sc.parallelizePairs(list3);

//        list1RDD.cogroup(list2RDD,list3RDD).foreach(new VoidFunction<Tuple2<Integer, Tuple3<Iterable<String>, Iterable<String>, Iterable<String>>>>() {
//            @Override
//            public void call(Tuple2<Integer, Tuple3<Iterable<String>, Iterable<String>, Iterable<String>>> tuple) throws Exception {
//                System.out.println(tuple._1+" " +tuple._2._1() +" "+tuple._2._2()+" "+tuple._2._3());
//            }
//        });

        list1RDD.cogroup(list2RDD,list3RDD)
                .foreach(tuple -> System.out.println(tuple));
        sc.stop();

    }

    public static void sortByKey(){
        List<Tuple2<Integer, String>> list = Arrays.asList(
                new Tuple2<>(99, "张三丰"),
                new Tuple2<>(96, "东方不败"),
                new Tuple2<>(66, "林平之"),
                new Tuple2<>(98, "聂风")
        );
//        JavaPairRDD<Integer, String> listRDD = sc.parallelizePairs(list);
//        listRDD.sortByKey(false).foreach(new VoidFunction<Tuple2<Integer, String>>() {
//            @Override
//            public void call(Tuple2<Integer, String> tuple) throws Exception {
//                System.out.println(tuple._2+"->"+tuple._1);
//            }
//        });
        sc.parallelizePairs(list)
                .sortByKey(false)
                .foreach(tuple -> System.out.println(tuple));
        sc.stop();

    }


    /**
     * aggregateByKey函数对PairRDD中相同Key的值进行聚合操作，在聚合过程中同样使用了一个中立的初始值。
     * 和aggregate函数类似，aggregateByKey返回值的类型不需要和RDD中value的类型一致。
     * 因为aggregateByKey是对相同Key中的值进行聚合操作，所以aggregateByKey函数最终返回的类型还是Pair RDD，
     * 对应的结果是Key和聚合好的值；而aggregate函数直接是返回非RDD的结果，这点需要注意。
     * 在实现过程中，定义了三个aggregateByKey函数原型，但最终调用的aggregateByKey函数都一致。
     *
     */
    public static void aggregateByKey(){
        List<String> list = Arrays.asList("you,jump", "i,jump");
//        JavaRDD<String> listRDD = sc.parallelize(list);
//        listRDD.flatMap(new FlatMapFunction<String, String>() {
//            @Override
//            public Iterator<String> call(String line) throws Exception {
//                return Arrays.asList(line.split(",")).iterator();
//            }
//        }).mapToPair(new PairFunction<String, String, Integer>() {
//            @Override
//            public Tuple2<String, Integer> call(String word) throws Exception {
//                return new Tuple2<>(word,1);
//            }
//        }).aggregateByKey(0, new Function2<Integer, Integer, Integer>() {
//            @Override
//            public Integer call(Integer i1, Integer i2) throws Exception {
//                return i1 + i2;
//            }
//        }, new Function2<Integer, Integer, Integer>() {
//            @Override
//            public Integer call(Integer i1, Integer i2) throws Exception {
//                return i1+i2;
//            }
//        }).foreach(new VoidFunction<Tuple2<String, Integer>>() {
//            @Override
//            public void call(Tuple2<String, Integer> tuple) throws Exception {
//                System.out.println(tuple._1+"->"+tuple._2);
//            }
//        });


        JavaRDD<String> listRDD = sc.parallelize(list);
        listRDD.flatMap(line -> Arrays.asList(line.split(",")).iterator())
                .mapToPair(word -> new Tuple2<>(word,1))
                .aggregateByKey(
                        0,
                        (i1, i2) -> i1 + i2,
                        (i1, i2) -> i1+i2)
                .foreach(tuple -> System.out.println(tuple));
        sc.stop();
    }


}
