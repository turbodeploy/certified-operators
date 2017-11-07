package com.vmturbo.components.api.client;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;

import com.vmturbo.components.api.BaseKafkaConfig;

/**
 * Base Kafka client configuration. This confuguration should be imported from Srpring context in
 * order to create connsumer bean. Consumer bean should be in only one instance (singleton)
 * accross the component.
 */
@Configuration
public class BaseKafkaConsumerConfig extends BaseKafkaConfig {

    /**
     * Kafka consumer group id.
     */
    @Value("${instance_id}")
    private String consumerGroup;

    /**
     * Lazily creates kafka consumer.
     *
     * @return kafka consumer
     */
    @Bean
    @Lazy
    public KafkaMessageConsumer kafkaConsumer() {
        return new KafkaMessageConsumer(bootstrapServer(), consumerGroup);
    }

    @Bean
    public KafkaConsumerStarter startKafka() {
        return new KafkaConsumerStarter();
    }
}
