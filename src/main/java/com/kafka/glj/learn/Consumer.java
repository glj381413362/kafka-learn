package com.kafka.glj.learn;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

public class Consumer {

    public static void main(String[] args) {
        Properties props = new Properties();
        // 配置broker地址   broker （server 9092  nio）
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.3.14:9092");
        // 消费组
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "DemoConsumer2");
        // 是否自动提交offset
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        // 自动提交offset的间隔时间
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        /*
      心跳时间，服务端broker通过心跳确认consumer是否故障，如果发现故障，就会通过心跳下发
      rebalance的指令给其他的consumer通知他们进行rebalance操作，这个时间可以稍微短一点
      */
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "1000");
        //服务端broker多久感知不到一个consumer心跳就认为他故障了，默认是10秒
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");


        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // 获取consumer
        KafkaConsumer<Integer, String> consumer = new KafkaConsumer(props);

        String topicName = "topicTest2";

        //consumer.subscribe(Arrays.asList(topicName));
        // 指定分区
        consumer.assign(Arrays.asList(new TopicPartition(topicName, 0)));
        //消息回溯消费
        //consumer.seekToBeginning(Arrays.asList(new TopicPartition(topicName, 0)));
        //指定offset消费
       consumer.seek(new TopicPartition(topicName, 0), 10);
       // 根据时间戳

        while(true){
            /*
             * poll() API 是拉取消息的长轮询，主要是判断consumer是否还活着，只要我们持续调用poll()，
             * 消费者就会存活在自己所在的group中，并且持续的消费指定partition的消息。
             * 底层是这么做的：消费者向server持续发送心跳，如果一个时间段（session.
             * timeout.ms）consumer挂掉或是不能发送心跳，这个消费者会被认为是挂掉了，
             * 这个Partition也会被重新分配给其他consumer
             */

            ConsumerRecords<Integer, String> records = consumer.poll(1000);
            for (ConsumerRecord<Integer, String> record : records) {
                System.out.println("Received message: (" + record.key() + ", " + record.value() + ") at offset " + record.offset());
            }

//            if (records.count() > 0) {
//                // 提交offset
//                consumer.commitSync();
//            }
        }


    }
}
