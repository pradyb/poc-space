package com.prady.sample.kafka.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ConsumerAwareListenerErrorHandler;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.RecordInterceptor;
import org.springframework.kafka.support.serializer.DeserializationException;
import org.springframework.lang.NonNull;
import org.springframework.util.backoff.FixedBackOff;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Author: Pradeep Balakrishnan
 */
@Configuration
public class KafkaConfig {

    private static final Logger logger = LoggerFactory.getLogger(KafkaConfig.class);

    @Value("${kafka.topic.demo}")
    private String demoTopicName;

    @Bean
    public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String, String>> kafkaListenerContainerFactory(
            ConsumerFactory<String, String> consumerFactory) {
        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.setConcurrency(3);
        factory.setRecordInterceptor(processingTimeInterceptor());
        
        // Configure error handling with DefaultErrorHandler (new approach in Spring Kafka)
        DefaultErrorHandler errorHandler = new DefaultErrorHandler((consumerRecord, exception) -> {
            if (exception instanceof DeserializationException deserializationException) {
                logger.error("Deserialization error: message={}", deserializationException.getMessage());
                // Access raw data if needed
                byte[] data = deserializationException.getData();
                if (data != null) {
                    logger.debug("Problem with message data of size: {}", data.length);
                }
            } else {
                logger.error("Error in listener: {}", exception.getMessage(), exception);
            }
        }, new FixedBackOff(1000L, 0)); // No retries for deserialization errors
        
        factory.setCommonErrorHandler(errorHandler);
        return factory;
    }
    
    @Bean(name = "kafkaListenerErrorHandler")
    public ConsumerAwareListenerErrorHandler errorHandler() {
        return (message, exception, consumer) -> {
            if (exception.getCause() instanceof DeserializationException deserializationException) {
                // Log the deserialization error
                logger.error("Error deserializing message: {}", deserializationException.getMessage());
                
                // Access the raw data if needed
                byte[] data = deserializationException.getData();
                if (data != null) {
                    logger.debug("Problematic message length: {}", data.length);
                }
                
                // In newer versions we can extract headers
                Object headers = deserializationException.getHeaders();
                if (headers != null) {
                    logger.debug("Message had headers: {}", headers);
                }
            } else {
                logger.error("Listener execution failed", exception);
            }
            
            // Return an empty object instead of null to comply with @NonNull requirement
            return new Object();
        };
    }

     /**
     * Bean that provides a RecordInterceptor for Kafka consumers.
     * <p>
     * This interceptor tracks the processing time for each consumed Kafka record.
     * It logs when processing starts, when it succeeds (with the time taken), and when it fails (with the time taken and error message).
     * The processing times are tracked using a thread-safe ConcurrentHashMap, keyed by a unique identifier for each record (topic-partition-offset).
     *
     * @return a RecordInterceptor that logs processing times and errors for Kafka messages
     */
    @Bean
    public RecordInterceptor<String, String> processingTimeInterceptor() {
        return new RecordInterceptor<String, String>() {
            /**
             * Stores the start processing time for each record, keyed by topic-partition-offset.
             */
            private final Map<String, Long> processingStartTimes = new ConcurrentHashMap<>();

            /**
             * Helper method to generate a unique record identifier.
             */
            private String getRecordId(ConsumerRecord<String, String> consumerRecord) {
                return consumerRecord.topic() + "-" + consumerRecord.partition() + "-" + consumerRecord.offset();
            }

            @Override
            public ConsumerRecord<String, String> intercept(@NonNull ConsumerRecord<String, String> consumerRecord,
                                                            @NonNull Consumer<String, String> consumer) {
                String recordId = getRecordId(consumerRecord);
                processingStartTimes.put(recordId, System.currentTimeMillis());
                logger.info("Starting to process message: topic={}, partition={}, offset={}, key={}", consumerRecord.topic(), consumerRecord.partition(), consumerRecord.offset(), consumerRecord.key());
                return consumerRecord;
            }

            @Override
            public void success(@NonNull ConsumerRecord<String, String> consumerRecord,
                                @NonNull Consumer<String, String> consumer) {
                String recordId = getRecordId(consumerRecord);
                Long startTime = processingStartTimes.remove(recordId);
                if (startTime != null) {
                    long processingTime = System.currentTimeMillis() - startTime;
                    logger.info("Successfully processed message: topic={}, partition={}, offset={}, key={} in {} ms", consumerRecord.topic(), consumerRecord.partition(), consumerRecord.offset(), consumerRecord.key(), processingTime);
                }
            }

            @Override
            public void failure(@NonNull ConsumerRecord<String, String> consumerRecord,
                                @NonNull Exception exception,
                                @NonNull Consumer<String, String> consumer) {
                String recordId = getRecordId(consumerRecord);
                Long startTime = processingStartTimes.remove(recordId);
                if (startTime != null) {
                    long processingTime = System.currentTimeMillis() - startTime;
                    logger.error("Failed to process message: topic={}, partition={}, offset={}, key={} after {} ms. Error: ", consumerRecord.topic(), consumerRecord.partition(), consumerRecord.offset(), consumerRecord.key(), processingTime, exception);
                }
            }
        };
    }

    @Bean
    public NewTopic topicDemo() {
        return TopicBuilder.name(demoTopicName)
                .partitions(1)
                .replicas(1)
                .build();
    }
}
