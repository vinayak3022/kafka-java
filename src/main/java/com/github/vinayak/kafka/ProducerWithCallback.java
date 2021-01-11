package com.github.vinayak.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.Properties;

@SuppressWarnings("Duplicates")
public class ProducerWithCallback {

    public static void main(String[] args){
        Logger logger = LoggerFactory.getLogger(ProducerWithCallback.class);
        String BOOTSTRAP_SERVER = "127.0.0.1:9092";

        //create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //create producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        //create producer record
        for(int i = 0 ; i < 10 ; i++) {
            ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic", "hello world" + Integer.toString(i));


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
            });
        }

        //flush data
        producer.flush();

        //flush and close producer
        producer.close();
    }
}
