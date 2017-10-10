package com.vmturbo.components.api.server;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.vmturbo.components.api.BaseKafkaConfig;

/**
 * Base configuration for Kafka notifications sender. This configuration should be imported from
 * Spring contexts.
 */
@Configuration
public class BaseKafkaProducerConfig extends BaseKafkaConfig {

    /**
     * Kafka sender bean. only one instance is expected to exist in one component.
     *
     * @return kafka message sender.
     */
    @Bean
    public KafkaMessageProducer kafkaMessageSender() {
        return new KafkaMessageProducer(bootstrapServer());
    }
}
