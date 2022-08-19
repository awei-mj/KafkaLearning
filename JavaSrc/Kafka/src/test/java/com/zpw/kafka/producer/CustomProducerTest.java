package com.zpw.kafka.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class CustomProducerTest {
    @Test
    public void produce() {
        //0 配置
        Properties properties = new Properties();
        //连接集群
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"ipaddr:9092");
        //指定对应的key和value的serializer
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        //1 创建Kafka生产者对象
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);
        //2 发送数据
        for (int i = 0; i < 5; i++) {
            kafkaProducer.send(new ProducerRecord<>("demo","hello"+i));
        }
        //3 关闭资源
        kafkaProducer.close();
    }

    @Test
    public void produceCallback() throws InterruptedException {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"ipaddr:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);
        for (int i = 0; i < 50; i++) {
            kafkaProducer.send(new ProducerRecord<>("demo", "hello" + i),
                    new Callback() {
                        @Override
                        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                            if(e==null) {
                                System.out.println("主题: "+recordMetadata.topic());
                                System.out.println("分区: "+recordMetadata.partition());
                            }
                        }
                    });

            Thread.sleep(5);
        }
        kafkaProducer.close();
    }

    @Test
    public void produceSync() throws ExecutionException, InterruptedException {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"ipaddr:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);
        for (int i = 0; i < 5; i++) {
            kafkaProducer.send(new ProducerRecord<>("demo", "hello" + i)).get();
        }
        kafkaProducer.close();
    }

    @Test
    public void produceCallbackPartitions() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"ipaddr:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        //关联自定义分区器
        properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG,MyPartitioner.class.getName());

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);
        for (int i = 0; i < 5; i++) {
            kafkaProducer.send(new ProducerRecord<>("demo",0,"", "hello" + i),
                    new Callback() {
                        @Override
                        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                            if(e==null) {
                                System.out.println("主题: "+recordMetadata.topic());
                                System.out.println("分区: "+recordMetadata.partition());
                            }
                        }
                    });
        }
        kafkaProducer.close();
    }

    @Test
    public void produceParameters() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"ipaddr:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        //缓冲区大小 64M
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 0x4000000);
        //批次大小 32K
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, 0x8000);
        //linger.ms
        properties.put(ProducerConfig.LINGER_MS_CONFIG, 5);
        //压缩 默认none,可配置值gzip、snappy、lz4和zstd
        properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);
        for (int i = 0; i < 5; i++) {
            kafkaProducer.send(new ProducerRecord<>("demo", "hello" + i));
        }
        kafkaProducer.close();
    }

    @Test
    public void produceAcks() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"ipaddr:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        //Acks
        properties.put(ProducerConfig.ACKS_CONFIG, "1");
        //重试次数 默认最大值0x7fffffff
        properties.put(ProducerConfig.RETRIES_CONFIG, 3);

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);
        for (int i = 0; i < 5; i++) {
            kafkaProducer.send(new ProducerRecord<>("demo", "hello" + i));
        }
        kafkaProducer.close();
    }

    @Test
    public void produceTransactions() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"ipaddr:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        //指定transactional.id
        properties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "transaction_id_MSI");

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);

        kafkaProducer.initTransactions();
        kafkaProducer.beginTransaction();

        try {
            for (int i = 0; i < 5; i++) {
                kafkaProducer.send(new ProducerRecord<>("demo", "hello" + i));
            }
            if (true)
                throw new ProducerFencedException("");
            kafkaProducer.commitTransaction();
        } catch (ProducerFencedException e) {
            kafkaProducer.abortTransaction();
            System.out.println();
        } finally {
            kafkaProducer.close();
        }
    }

}
