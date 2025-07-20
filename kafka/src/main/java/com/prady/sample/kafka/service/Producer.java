package com.prady.sample.kafka.service;

import com.prady.sample.kafka.model.Message;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

/**
 * @author Pradeep Balakrishnan
 */
@Service
public class Producer {

    private final KafkaTemplate<String, Message> kafkaTemplate;
    private final String topic;

    public Producer(KafkaTemplate<String, Message> kafkaTemplate,
                    @Value("${kafka.topic.demo}") String topic) {
        this.kafkaTemplate = kafkaTemplate;
        this.topic = topic;
    }

    public void sendMessage(Message message) {
        kafkaTemplate.send(topic, message.getContent(), message);
    }
}