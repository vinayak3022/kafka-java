package com.github.vinayak.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

@SuppressWarnings("Duplicates")
public class ConsumerWithThreads {
    public static void main(String[] args){
        new ConsumerWithThreads().run();
    }

    private ConsumerWithThreads(){}

    private void run(){
        Logger logger = LoggerFactory.getLogger(ConsumerWithThreads.class.getName());

        String BOOTSTRAP_SERVER = "127.0.0.1:9092";
        String groupId = "my-fourth-application";
        String topic = "first_topic";

        //latch dealing with mutliple threads
        CountDownLatch latch = new CountDownLatch(1);

        //create consumber runnable
        Runnable myConsumerRunnable = new ConsumerThread(topic,
                BOOTSTRAP_SERVER,
                groupId,
                latch);

        // start the thread
        Thread mythread = new Thread(myConsumerRunnable);
        mythread.start();

        //add a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread( () -> {
            logger.info("Caught shutdown hook");
            ((ConsumerThread) myConsumerRunnable).shutdown();

            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        ));

        try {
            latch.await();
        }catch (InterruptedException e) {
            logger.error("Application got interrupted", e);
        }finally {
            logger.info("Application is closing");
        }
    }

    public class ConsumerThread implements Runnable{

        private CountDownLatch latch;
        private KafkaConsumer<String, String> consumer;
        private Logger logger = LoggerFactory.getLogger(ConsumerThread.class.getName());



        public ConsumerThread(String topic,
                              String BOOTSTRAP_SERVER,
                              String groupId,
                              CountDownLatch latch){
            this.latch = latch;

            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

            consumer = new KafkaConsumer<String, String>(properties);

            consumer.subscribe(Arrays.asList(topic));
        }

        @Override
        public void run() {
            //poll for new data
            try{
                while (true){
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                    for(ConsumerRecord<String, String> record : records){
                        logger.info("Key: "+record.key() +" ,Value: "+ record.value() + "\n");
                        logger.info("Partition: "+record.partition() +" ,Offset: "+record.offset());
                    }
                }
            }catch (WakeupException e){
                logger.info("Received the shutdown signal");
            }finally {
                consumer.close();
                latch.countDown();
            }
        }

        public void shutdown(){
            consumer.wakeup();
        }
    }
}
