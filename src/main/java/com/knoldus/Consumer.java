package com.knoldus;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class Consumer {

    public static void main(String[] args) {

        ConsumerListener consumerListener = new ConsumerListener();
        Thread thread = new Thread(consumerListener);
        thread.start();
    }
    public static void consumer(){
        Properties properties=new Properties();
        properties.put("bootstrap.servers","localhost:9092");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "com.knoldus.UserDeserealizer");
        properties.put("group.id","test-group");

        KafkaConsumer<String, User> kafkaConsumer = new KafkaConsumer(properties);
        List topics = new ArrayList();
        topics.add("users");
        kafkaConsumer.subscribe(topics);

        try{
            while (true){
                ConsumerRecords<String,User> consumerRecord=kafkaConsumer.poll(10000);
                for (ConsumerRecord<String,User> record:consumerRecord){
                    System.out.println("Topic:"+record.topic()+" Key:"+record.key()+" Value:"+record.value());
                    writeFile(record.value().toString());
                }
            }
        }
        catch (Exception e){
            e.printStackTrace();
        }
        finally {
            kafkaConsumer.close();
        }
    }

    public static void writeFile(String data) {
        OutputStream outputStream = null;
        try {
            outputStream = new FileOutputStream(new File("/home/knoldus/Desktop/Kafka_File/record.txt"));
            outputStream.write(data.getBytes(), 0, data.length());
        } catch (IOException ioException) {
            ioException.printStackTrace();
        } finally {
            try {
                outputStream.close();
            } catch (IOException io) {
                io.printStackTrace();
            }
        }
    }
}


class ConsumerListener implements Runnable{

    @Override
    public void run() {
        Consumer.consumer();
    }
}
