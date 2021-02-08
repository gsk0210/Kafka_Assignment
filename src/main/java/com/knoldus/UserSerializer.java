package com.knoldus;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class UserSerializer implements Serializer<User> {


    @Override
    public void configure(Map configs, boolean isKey) {

    }


    @Override
    public byte[] serialize(String topic,User user) {
        byte[] returnValue=null;
        ObjectMapper objectMapper=new ObjectMapper();
        try {
            returnValue=objectMapper.writeValueAsString(user).getBytes();
        }
        catch(Exception e){
            e.printStackTrace();
        }
        return returnValue;
    }

    @Override
    public void close() {

    }
}
