package com.github.vinayak.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

@SuppressWarnings("Duplicates")
public class ProducerKeys {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Logger logger = LoggerFactory.getLogger(ProducerKeys.class);
        String BOOTSTRAP_SERVER = "127.0.0.1:9092";

        //create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //create producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        //create producer record
        for(int i = 1 ; i <= 10 ; i++) {

            String topic = "first_topic";
            String value = "hello world" + Integer.toString(i);
            String key = "id_"+Integer.toString(i);

            ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key, value);

            logger.info("Key: "+key);

            //send data - asynchronous
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    //executes every time a record is successfull or when an exception

                    if (Objects.isNull(e)) {
                        logger.info("Metadata: \n" +
                                "Topic: " + recordMetadata.topic() + "\n" +
                                "Partition: " + recordMetadata.partition() + "\n" +
                                "Offset: " + recordMetadata.offset() + "\n" +
                                "Timestamp :" + recordMetadata.timestamp());
                    } else {
                        logger.error("Error while producing", e);
                    }
                }
            }).get(); //block send to make it synchronous
        }

        //flush data
        producer.flush();

        //flush and close producer
        producer.close();
    }
}
