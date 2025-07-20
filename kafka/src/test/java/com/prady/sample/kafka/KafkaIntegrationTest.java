package com.prady.sample.kafka;

import com.prady.sample.kafka.model.Message;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;

@SpringBootTest
@EmbeddedKafka(partitions = 1, topics = {"demo-topic"}, ports = {9096})
class KafkaIntegrationTest {

    @Autowired
    private EmbeddedKafkaBroker embeddedKafka;

    @Autowired
    private KafkaTemplate<String, Message> kafkaTemplate;

    @Autowired
    private ConsumerFactory<String, Message> consumerFactory;

    @Test
    void testWhenSendMessageThenReceive() {
        embeddedKafka.consumeFromAnEmbeddedTopic(consumerFactory.createConsumer(), "demo-topic");
        Message message = new Message("integration-test");
        kafkaTemplate.send("demo-topic", message);
        ConsumerRecords<String, Message> records = KafkaTestUtils.getRecords(consumerFactory.createConsumer());
        Assertions.assertFalse(records.isEmpty());
        Assertions.assertEquals("integration-test", records.iterator().next().value().getContent());
    }
}