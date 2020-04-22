package com.vmturbo.components.api.server;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;

import com.vmturbo.components.api.BaseKafkaConfig;

/**
 * Base configuration for Kafka notifications sender. This configuration should be imported from
 * Spring contexts.
 */
@Configuration
public class BaseKafkaProducerConfig extends BaseKafkaConfig {

    /**
     * We will NOT send any messages larger than this size (they will be rejected by Kafka).
     *
     * <p>The default is 64MB.
     */
    @Value("${kafkaMaxRequestSizeBytes:67108864}")
    private int maxRequestSizeBytes;

    /**
     * We will try to send chunks of this size.
     *
     * <p>The default is 124KB. Some preliminary reading suggests this is a reasonable size for high
     * throughput, but we should do more experiments to tweak.
     */
    @Value("${kafkaRecommendedRequestSizeBytes:126976}")
    private int recommendedRequestSizeBytes;



    /**
     * Kafka sender bean. only one instance is expected to exist in one component.
     *
     * @return kafka message sender.
     */
    @Bean
    @Lazy
    public KafkaMessageProducer kafkaMessageSender() {
        return new KafkaMessageProducer(bootstrapServer(), kafkaNamespacePrefix(),
            maxRequestSizeBytes, recommendedRequestSizeBytes);
    }
}
