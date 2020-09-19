package com.pmw.kafkaproj.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class ProducerSample {

    private final static String TOPIC_NAME = "pmw-topic";

    /*
        Producer Async
     */
    public static void producerSend(){

        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"192.168.152.131:9092");
        properties.put(ProducerConfig.ACKS_CONFIG,"all");
        properties.put(ProducerConfig.RETRIES_CONFIG,"0");
        // use default
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG,"16384");
        properties.put(ProducerConfig.LINGER_MS_CONFIG,"1");
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG,"33554432");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization");

        // Producer API
        Producer<String, String> producer = new KafkaProducer<String, String>(properties);

        //ProducerRecorder
        for(int i=0;i<10;i++){
            ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME,"key-"+i,"value-"+i);
            producer.send(record);
        }

        // must close!
        producer.close();
    }

    public static void main(String args){
        //async send
        producerSend();
    }


}
