package com.vmturbo.components.api.client;

import java.util.Optional;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;

import com.vmturbo.components.api.BaseKafkaConfig;
import com.vmturbo.components.api.localbus.LocalBus;

/**
 * Base Kafka client configuration. This configuration should be imported from Spring context in
 * order to create consumer bean. Consumer bean should be in only one instance (singleton)
 * across the component.
 */
@Configuration
public class BaseKafkaConsumerConfig extends BaseKafkaConfig {

    /**
     * Kafka consumer group id.
     */
    @Value("${consumer_group:${component_type}}")
    private String consumerGroup;

    /**
     * Lazily creates kafka consumer.
     *
     * @return kafka consumer
     */
    @Bean
    @Lazy
    public IMessageReceiverFactory kafkaConsumer() {
        if (useLocalBus()) {
            return LocalBus.getInstance();
        } else {
            return new KafkaMessageConsumer(bootstrapServer(), consumerGroup,
                    kafkaNamespacePrefix(), Optional.of(kafkaTlsProperty()));
        }

    }

    @Bean
    public KafkaConsumerStarter startKafka() {
        return new KafkaConsumerStarter(useLocalBus());
    }
}
