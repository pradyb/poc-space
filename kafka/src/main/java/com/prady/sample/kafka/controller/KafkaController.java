package com.prady.sample.kafka.controller;

import com.prady.sample.kafka.model.Message;
import com.prady.sample.kafka.service.Producer;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author Pradeep Balakrishnan
 */
@RestController
@RequestMapping("/kafka")
public class KafkaController {

    private final Producer producer;

    public KafkaController(Producer producer) {
        this.producer = producer;
    }

    @PostMapping("/publish")
    public ResponseEntity<String> publishMessage(@RequestBody Message message,
                                                 @RequestParam(value = "count", defaultValue = "1") int count) {
        String content = message.getContent();
        for (int i = 0; i < count; i++) {
            message.setContent(content + " " + (i + 1));
            producer.sendMessage(message);
        }
        return ResponseEntity.ok("Message sent to Kafka topic");
    }
}