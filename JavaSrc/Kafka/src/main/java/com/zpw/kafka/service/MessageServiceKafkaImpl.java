package com.zpw.kafka.service;

import org.apache.kafka.clients.producer.Producer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class MessageServiceKafkaImpl implements MessageService {

    @Autowired
    private KafkaTemplate<String,String> kafkaTemplate;

    @Override
    public void sendMessage(String id){

        kafkaTemplate.send("demo",0,"",id);
    }
    @Override
    public String doMessage(){
        return null;
    }
}
