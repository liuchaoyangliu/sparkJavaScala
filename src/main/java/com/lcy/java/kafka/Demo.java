package com.lcy.java.kafka;

import org.apache.hadoop.hive.ql.metadata.AuthorizationException;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.*;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Utils;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * 较复杂案例，未看懂
 */

public class Demo {
    
    public static void main(String[] args) throws InterruptedException {
        Properties props = new Properties();
        //设置kafka集群的地址
        props.put("bootstrap.servers", "hadoop01:9092,hadoop02:9092,hadoop03:9092");
        //ack模式，all是最慢但最安全的
        props.put("acks", "-1");
        //失败重试次数
        props.put("retries", 0);
        //每个分区未发送消息总字节大小（单位：字节），超过设置的值就会提交数据到服务端
        props.put("batch.size", 10);
        //props.put("max.request.size",10);
        //消息在缓冲区保留的时间，超过设置的值就会被提交到服务端
        props.put("linger.ms", 10000);
        //整个Producer用到总内存的大小，如果缓冲区满了会提交数据到服务端
        //buffer.memory要大于batch.size，否则会报申请内存不足的错误
        props.put("buffer.memory", 10240);
        //序列化器
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        Producer<String, String> producer = new KafkaProducer<>(props);
        for (int i = 0; i < 100; i++)
            producer.send(new ProducerRecord<>("mytopic1", Integer.toString(i), "dd:" + i));
        //Thread.sleep(1000000);
        producer.close();
    }
    
    //事务模式
    public void transactional() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "hadoop01:9092,hadoop02:9092,hadoop03:9092");
        props.put("transactional.id", "my_transactional_id");
        
        Producer<String, String> producer = new KafkaProducer<>(props,
                new StringSerializer(),
                new StringSerializer());
        
        producer.initTransactions();
        
        try {
            //数据发送必须在beginTransaction()和commitTransaction()中间，否则会报状态不对的异常
            producer.beginTransaction();
            
            for (int i = 0; i < 100; i++)
                producer.send(new ProducerRecord<>("mytopic1",
                        Integer.toString(i),
                        Integer.toString(i)));
            
            producer.commitTransaction();
        } catch (ProducerFencedException | OutOfOrderSequenceException | AuthorizationException e) {
            // 这些异常不能被恢复，因此必须要关闭并退出Producer
            producer.close();
        } catch (KafkaException e) {
            // 出现其它异常，终止事务
            producer.abortTransaction();
        }
        producer.close();
    }
    
    //自定义分区类(Partitioner)
    class DefaultPartitioner implements Partitioner {
        
        private final ConcurrentMap<String, AtomicInteger> topicCounterMap = new ConcurrentHashMap();
        
        //需要覆盖的方法
        public DefaultPartitioner() {
        }
        
        //需要覆盖的方法，可以在这里添加配置信息
        public void configure(Map<String, ?> configs) {
        }
        
        //需要覆盖的方法，最重要的
        /*
        topic:主题
        key:动态绑定的，传的什么类型就是什么类型
        keyBytes:Ascii码数组
        value:动态绑定的，传的什么类型就是什么类型
        valueBytes:Ascii码数组
        cluster:kafka集群
        */
        public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes,
                             Cluster cluster) {
            //拿到所有分区
            List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
            //分区数量
            int numPartitions = partitions.size();
            //如果key为空，则取消息作为分区依据
            if (keyBytes == null) {
                int nextValue = this.nextValue(topic);
                //可用分区，我在想应该是
                List<PartitionInfo> availablePartitions = cluster.availablePartitionsForTopic(topic);
                //可用分区数
                if (availablePartitions.size() > 0) {
                    //计算分区索引
                    int part = Utils.toPositive(nextValue) % availablePartitions.size();
                    //返回分区
                    return ((PartitionInfo) availablePartitions.get(part)).partition();
                } else {
                    //如果可用分区=0，则直接返回所有分区中的一个
                    return Utils.toPositive(nextValue) % numPartitions;
                }
            } else {
                //key有值，则返回所有分区中的一个
                return Utils.toPositive(Utils.murmur2(keyBytes)) % numPartitions;
            }
        }
        
        //如果没有key，则调用该方法那消息来做分区依据
        private int nextValue(String topic) {
            AtomicInteger counter = (AtomicInteger) this.topicCounterMap.get(topic);
            if (null == counter) {
                counter = new AtomicInteger(ThreadLocalRandom.current().nextInt());
                AtomicInteger currentCounter = (AtomicInteger) this.topicCounterMap.putIfAbsent(topic, counter);
                if (currentCounter != null) {
                    counter = currentCounter;
                }
            }
            
            return counter.getAndIncrement();
        }
        
        //需要覆盖的方法
        public void close() {
        }
    }
    
    /*
        消费者拉取数据之后自动提交偏移量，不关心后续对消息的处理是否正确
        优点：消费快，适用于数据一致性弱的业务场景
        缺点：消息很容易丢失
     */
    public void autoCommit() {
        Properties props = new Properties();
        //设置kafka集群的地址
        props.put("bootstrap.servers", "hadoop01:9092,hadoop02:9092,hadoop03:9092");
        //设置消费者组，组名字自定义，组名字相同的消费者在一个组
        props.put("group.id", "my_group");
        //开启offset自动提交
        props.put("enable.auto.commit", "true");
        //自动提交时间间隔
        props.put("auto.commit.interval.ms", "1000");
        //序列化器
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        //实例化一个消费者
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        //消费者订阅主题，可以订阅多个主题
        consumer.subscribe(Arrays.asList("mytopic1"));
        //死循环不停的从broker中拿数据
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records)
                System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
        }
    }
    
    //.偏移量-手动按消费者提交
    public void munualCommit() {
        Properties props = new Properties();
        //设置kafka集群的地址
        props.put("bootstrap.servers", "hadoop01:9092,hadoop02:9092,hadoop03:9092");
        //设置消费者组，组名字自定义，组名字相同的消费者在一个组
        props.put("group.id", "my_group");
        //开启offset自动提交
        props.put("enable.auto.commit", "false");
        //序列化器
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        //实例化一个消费者
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        //消费者订阅主题，可以订阅多个主题
        consumer.subscribe(Arrays.asList("mytopic1"));
        final int minBatchSize = 50;
        List<ConsumerRecord<String, String>> buffer = new ArrayList<>();
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                buffer.add(record);
            }
            if (buffer.size() >= minBatchSize) {
                //insertIntoDb(buffer);
                for (ConsumerRecord bf : buffer) {
                    System.out.printf("offset = %d, key = %s, value = %s%n", bf.offset(), bf.key(), bf.value());
                }
                consumer.commitSync();
                buffer.clear();
            }
        }
        
    }
    
    
    public void munualCommitByPartition() {
        Properties props = new Properties();
        //设置kafka集群的地址
        props.put("bootstrap.servers", "hadoop01:9092,hadoop02:9092,hadoop03:9092");
        //设置消费者组，组名字自定义，组名字相同的消费者在一个组
        props.put("group.id", "my_group");
        //开启offset自动提交
        props.put("enable.auto.commit", "false");
        //序列化器
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        //实例化一个消费者
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        //消费者订阅主题，可以订阅多个主题
        consumer.subscribe(Arrays.asList("mytopic3"));
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Long.MAX_VALUE);
                for (TopicPartition partition : records.partitions()) {
                    List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);
                    for (ConsumerRecord<String, String> record : partitionRecords) {
                        System.out.println("partition: " + partition.partition() + " , " + record.offset() + ": " + record.value());
                    }
                    long lastOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
                    /*
                        提交的偏移量应该始终是您的应用程序将要读取的下一条消息的偏移量。因此，在调用commitSync（）时，
                        offset应该是处理的最后一条消息的偏移量加1
                        为什么这里要加上面不加喃？因为上面Kafka能够自动帮我们维护所有分区的偏移量设置，
                        有兴趣的同学可以看看SubscriptionState.allConsumed()就知道
                     */
                    consumer.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(lastOffset + 1)));
                }
            }
        } finally {
            consumer.close();
        }
    }
    
    
    public void munualPollByPartition() {
        Properties props = new Properties();
        //设置kafka集群的地址
        props.put("bootstrap.servers", "hadoop01:9092,hadoop02:9092,hadoop03:9092");
        //设置消费者组，组名字自定义，组名字相同的消费者在一个组
        props.put("group.id", "my_group");
        //开启offset自动提交
        props.put("enable.auto.commit", "false");
        //序列化器
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        //实例化一个消费者
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        //消费者订阅主题，并设置要拉取的分区
        TopicPartition partition0 = new TopicPartition("mytopic3", 0);
        //TopicPartition partition1 = new TopicPartition("mytopic2", 1);
        //consumer.assign(Arrays.asList(partition0, partition1));
        consumer.assign(Arrays.asList(partition0));
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Long.MAX_VALUE);
                for (TopicPartition partition : records.partitions()) {
                    List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);
                    for (ConsumerRecord<String, String> record : partitionRecords) {
                        System.out.println("partition: " +
                                partition.partition() + " , " +
                                record.offset() + ": " +
                                record.value());
                    }
                    long lastOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
                    consumer.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(lastOffset + 1)));
                }
            }
        } finally {
            consumer.close();
        }
    }
    
    
    /*
        手动设置指定分区的offset，只适用于使用Consumer.assign方法添加主题的分区，不适用于kafka自动管理消费者组中的消费者场景，
        后面这种场景可以使用ConsumerRebalanceListener做故障恢复使用
     */
    public void controlsOffset() {
        Properties props = new Properties();
        //设置kafka集群的地址
        props.put("bootstrap.servers", "hadoop01:9092,hadoop02:9092,hadoop03:9092");
        //设置消费者组，组名字自定义，组名字相同的消费者在一个组
        props.put("group.id", "my_group");
        //开启offset自动提交
        props.put("enable.auto.commit", "false");
        //序列化器
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        //实例化一个消费者
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        //消费者订阅主题，并设置要拉取的分区
        
        //加一段代码将自己保存的分区和偏移量读取到内存
        //load partition and it's offset
        TopicPartition partition0 = new TopicPartition("mytopic3", 0);
        consumer.assign(Arrays.asList(partition0));
        
        //告知Consumer每个分区应该从什么位置开始拉取数据，offset从你加载的值或者集合中拿
        consumer.seek(partition0, 4140l);
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Long.MAX_VALUE);
                for (TopicPartition partition : records.partitions()) {
                    List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);
                    
                    for (ConsumerRecord<String, String> record : partitionRecords) {
                        System.out.println("partition: " + partition.partition() + " , " + record.offset() + ": " + record.value());
                    }
                    long lastOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
                    consumer.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(lastOffset + 1)));
                }
            }
        } finally {
            consumer.close();
        }
    }
    
    
    /*
        kafka提供了这个监听来处理分区的变化，区分被取消时调用onPartitionsRevoked方法；分区被分配时调用onPartitionsAssigned
     */
    class MyConsumerRebalanceListener implements ConsumerRebalanceListener {
        Map<TopicPartition, Long> partitionMap = new ConcurrentHashMap<>();
        private Consumer<?, ?> consumer;
        
        //实例化Listener的时候将Consumer传进来
        public MyConsumerRebalanceListener(Consumer<?, ?> consumer) {
            this.consumer = consumer;
        }
        
        /*
            有新的消费者加入消费者组或者已有消费者从消费者组中移除会触发kafka的rebalance机制，rebalance被调用前会先调用下面的方法
            此时你可以将分区和它的偏移量记录到外部存储中，比如DBMS、文件、缓存数据库等，还可以在这里处理自己的业务逻辑
         */
        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            for (TopicPartition partition : partitions) {
                //记录分区和它的偏移量
                partitionMap.put(partition, consumer.position(partition));
                //清空缓存
                
                System.out.println("onPartitionsRevoked partition:" + partition.partition() + " - offset" + consumer.position(partition));
            }
        }
        
        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            //设置分区的偏移量
            for (TopicPartition partition : partitions) {
                System.out.println("onPartitionsAssigned partition:" + partition.partition() + " - offset" + consumer.position(partition));
                if (partitionMap.get(partition) != null) {
                    consumer.seek(partition, partitionMap.get(partition));
                } else {
                    //自定义处理逻辑
                }
            }
        }
    }
    
    
    public void autoCommitAddListner() {
        Properties props = new Properties();
        //设置kafka集群的地址
        props.put("bootstrap.servers", "hadoop01:9092,hadoop02:9092,hadoop03:9092");
        //设置消费者组，组名字自定义，组名字相同的消费者在一个组
        props.put("group.id", "my_group");
        //开启offset自动提交 true-开启 false-关闭
        props.put("enable.auto.commit", "false");
        //序列化器
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        //实例化一个消费者
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        MyConsumerRebalanceListener myListener = new MyConsumerRebalanceListener(consumer);
        //消费者订阅主题，可以订阅多个主题
        consumer.subscribe(Arrays.asList("mytopic3"), myListener);
        //consumer.subscribe(Arrays.asList("mytopic3"));
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Long.MAX_VALUE);
                for (TopicPartition partition : records.partitions()) {
                    List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);
                    for (ConsumerRecord<String, String> record : partitionRecords) {
                        System.out.println("partition: " + partition.partition() + " , " + record.offset() + ": " + record.value());
                    }
                    long lastOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
                    /*
                        可以将这里的偏移量提交挪到监听的onPartitionsRevoked方法中，控制灵活，但是也很容易出问题
                     */
                    consumer.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(lastOffset + 1)));
                }
            }
        } finally {
            consumer.close();
        }
    }
    
    class ConsumerRunner implements Runnable {
        private final AtomicBoolean closed = new AtomicBoolean(false);
        private final KafkaConsumer<String, String> consumer;
        private final CountDownLatch latch;
        
        public ConsumerRunner(KafkaConsumer<String, String> consumer, CountDownLatch latch) {
            this.consumer = consumer;
            this.latch = latch;
        }
        
        @Override
        public void run() {
            System.out.println("threadName...." + Thread.currentThread().getName());
            try {
                consumer.subscribe(Arrays.asList("mytopic3"));
                while (!closed.get()) {
                    ConsumerRecords<String, String> records = consumer.poll(10000);
                    for (ConsumerRecord<String, String> record : records)
                        System.out.printf("threadName= %s, offset = %d, key = %s, value = %s%n",
                                Thread.currentThread().getName(), record.offset(), record.key(), record.value());
                }
            } catch (WakeupException e) {
                if (!closed.get()) throw e;
            } finally {
                consumer.close();
                latch.countDown();
            }
        }
        
        public void shutdown() {
            System.out.println("close ConsumerRunner");
            closed.set(true);
            consumer.wakeup();
        }
    }
    
    
    public void autoCommitParallelTest() {
        Properties props = new Properties();
        //设置kafka集群的地址
        props.put("bootstrap.servers", "hadoop01:9092,hadoop02:9092,hadoop03:9092");
        //设置消费者组，组名字自定义，组名字相同的消费者在一个组
        props.put("group.id", "my_group");
        //开启offset自动提交
        props.put("enable.auto.commit", "true");
        //自动提交时间间隔
        props.put("auto.commit.interval.ms", "1000");
        //序列化器
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        //实例化一个消费者
        final List<ConsumerRunner> consumers = new ArrayList<>();
        final List<KafkaConsumer<String, String>> kafkaConsumers = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            kafkaConsumers.add(new KafkaConsumer<>(props));
        }
        final CountDownLatch latch = new CountDownLatch(2);
        final ExecutorService executor = Executors.newFixedThreadPool(2);
        for (int i = 0; i < 2; i++) {
            ConsumerRunner c = new ConsumerRunner(kafkaConsumers.get(i), latch);
            consumers.add(c);
            executor.submit(c);
        }

        /*
            这个方法的意思就是在jvm中增加一个关闭的钩子，当jvm关闭的时候，会执行系统中已经设置的所有通过方法addShutdownHook添加的钩子，
            当系统执行完这些钩子后，jvm才会关闭
            所以这些钩子可以在jvm关闭的时候进行内存清理、对象销毁、关闭连接等操作
         */
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                System.out.println("....................");
                for (ConsumerRunner consumer : consumers) {
                    consumer.shutdown();
                }
                executor.shutdown();
                try {
                    executor.awaitTermination(5000, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
        
        try {
            latch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
    
}
