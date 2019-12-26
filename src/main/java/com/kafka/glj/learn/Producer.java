package com.kafka.glj.learn;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;


import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class Producer {

    public static void main(String[] args) {
        Properties properties = new Properties();
        // 配置 broker 地址
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"192.168.3.14:9092");
        /**
         发出消息持久化机制参数  默认是1
        （1）acks=0： 表示producer不需要等待任何broker确认收到消息的回复，就可以继续发送下一条消息。
         性能最高，但是最容易丢消息。
        （2）acks=1： 至少要等待leader已经成功将数据写入本地log，但是不需要等待所有follower
         是否成功写入。就可以继续发送下一条消息。这种情况下，如果follower没有成功备份数据，
         而此时leader又挂掉，则消息会丢失。
        （3）acks=-1或all： 这意味着leader需要等待所有备份(min.insync.replicas配置的备份个数)
         都成功写入日志，这种策略会保证只要有一个备份存活就不会丢失数据。
         这是最强的数据保证。一般除非是金融级别，或跟钱打交道的场景才会使用这种配置。
        */
        properties.put(ProducerConfig.ACKS_CONFIG,"1");

        //发送失败会重试，默认重试间隔100ms，重试能保证消息发送的可靠性，但是也可能造成消息
        // 重复发送，比如网络抖动，所以需要在接收者那边做好消息接收的幂等性处理
        properties.put(ProducerConfig.RETRIES_CONFIG, 3);
        //重试间隔设置
        properties.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 300);
        //设置发送消息的本地缓冲区，如果设置了该缓冲区，消息会先发送到本地缓冲区，可以提高消息发送性能，
        // 默认值是33554432，即32MB
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        //kafka本地线程会从缓冲区取数据，批量发送到broker，
        //设置批量发送消息的大小，默认值是16384，即16kb，就是说一个batch满了16kb就发送出去
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        //默认值是0，意思就是消息必须立即被发送，但这样会影响性能
        //一般设置100毫秒左右，就是说这个消息发送完后会进入本地的一个batch，如果100毫秒内，
        // 这个batch满了16kb就会随batch一起被发送出去
        //如果100毫秒内，batch没满，那么也必须把消息发送出去，不能让消息的发送延迟时间太长
        properties.put(ProducerConfig.LINGER_MS_CONFIG, 100);

        //把发送的key从字符串序列化为字节数组
        // 序列化操作是在拦截器（Interceptor）执行之后并且在分配分区(partitions)之前执行的
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        //把发送消息value从字符串序列化为字节数组
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // 拦截器
        properties.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG,"com.kafka.glj.learn.ProducerInterceptorDemo");

        //获取到producer
        // 必须配的参数  bootstrap.servers、key.serializer、value.serializer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);


        int messageNo = 1;
        boolean isAsync = false;
        while (true){
            String messageStr = "Message_" + messageNo;
            long startTime = System.currentTimeMillis();

            //指定发送分区   也可以自定义分区规则实现Partitioner#partition
            ProducerRecord record = new ProducerRecord<>("topicTest2",0,messageNo+"",messageStr);

            if (isAsync) { // Send asynchronously
                producer.send(record, new DemoCallBack(startTime, messageNo, messageStr));
            } else { // Send synchronously
                try {
                    producer.send(record).get();
                    System.out.println("Sent message: (" + messageNo + ", " + messageStr + ")");
                } catch (InterruptedException | ExecutionException e) {
                    e.printStackTrace();
                }
            }
            if (messageNo==10){
                break;
            }
            ++messageNo;

        }

        try {
            Thread.sleep(4000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        producer.close();
    }
}


class DemoCallBack implements Callback {

    private final long startTime;
    private final int key;
    private final String message;

    public DemoCallBack(long startTime, int key, String message) {
        this.startTime = startTime;
        this.key = key;
        this.message = message;
    }

    /**
     * A callback method the user can implement to provide asynchronous handling of request completion. This method will
     * be called when the record sent to the server has been acknowledged. Exactly one of the arguments will be
     * non-null.
     *
     * @param metadata  The metadata for the record that was sent (i.e. the partition and offset). Null if an error
     *                  occurred.
     * @param exception The exception thrown during processing of this record. Null if no error occurred.
     */
    @Override
    public void onCompletion(RecordMetadata metadata, Exception exception) {
        long elapsedTime = System.currentTimeMillis() - startTime;
        if (metadata != null) {
            System.out.println(
                    "message(" + key + ", " + message + ") sent to partition(" + metadata.partition() +
                            "), " +
                            "offset(" + metadata.offset() + ") in " + elapsedTime + " ms");
        } else {
            exception.printStackTrace();
        }
    }
}