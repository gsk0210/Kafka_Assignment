package com.knoldus;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class Producer {

    public static void main(String[] args) {
        Properties properties=new Properties();
        properties.put("bootstrap.servers","localhost:9092");
        properties.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer","com.knoldus.UserSerializer");

        KafkaProducer <String,User> kafkaProducer=new KafkaProducer<>(properties);
        try{
            for(int i=1;i<5;i++){
                User user=new User(i,"Name-"+i,20+i,"B.Tech");
                kafkaProducer.send(new ProducerRecord<String,User>("users",Integer.toString(i),user));
            }
        }
        catch (Exception exception){
            exception.printStackTrace();
        }
        finally {
            kafkaProducer.close();
        }
    }
}
