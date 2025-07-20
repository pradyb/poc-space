package com.prady.sample.kafka.service;

import com.prady.sample.kafka.model.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

/**
 * @author Pradeep Balakrishnan
 */
@Service
public class Consumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(Consumer.class);

    @KafkaListener(
        topics = "${kafka.topic.demo}", 
        groupId = "demo-group", 
        errorHandler = "kafkaListenerErrorHandler"
    )
    public void listen(Message message) throws InterruptedException {
        Thread.sleep(5000); // Simulate processing time
        LOGGER.info("Processed Message: {}", message.getContent());
    }
}