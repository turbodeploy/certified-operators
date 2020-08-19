package com.vmturbo.components.api.server;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;

import com.vmturbo.components.api.BaseKafkaConfig;
import com.vmturbo.components.api.localbus.LocalBus;

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
     * The maximum amount of time a kafka producer should wait for topic metadata before attempting a
     * send. If this is exceeded, the request will fail and potentially trigger a retry. Because we
     * have some known cases where we expect kafka to be unavailable for an extended period (mainly
     * upgrades and VM restarts), we are setting this to a generous 5 min.
     */
    @Value("${kafkaMaxBlockMs:300000}")
    private int maxBlockMs;

    /**
     * The total length of time given to the kafka producer to attempt to send a message in. This
     * includes internal retry attempts, which we are going to allow an unbounded number of. We want
     * kafka to own the retry logic (which is mostly does anyways) as much as possible, since us
     * manually controlling retries outside the producer does risk breaking idempotence. So we are going
     * to default to a 10 minute delivery window.
     */
    @Value("${kafkaDeliveryTimeoutMs:600000}")
    private int deliveryTimeoutMs;

    /**
     * The total time window, including both kafka internal retries as well as our own manual retry
     * attempts, that we will spend trying to send a single kafka message in. Kafka is very resilient
     * so if a send fails, it's almost always going to be an ephemeral condition that could have
     * eventually succeeded. Also, we have scenarios where we expect kafka to be unavailable for an
     * extended period, such as during upgrades or VM restarts, so we are going to default this to a
     * high number. Waiting longer on retries is preferable to failed sends.
     */
    @Value("${kafkaTotalSendRetrySecs:3600}")
    private int totalSendRetrySecs;

    /**
     * Kafka sender bean. only one instance is expected to exist in one component.
     *
     * @return kafka message sender.
     */
    @Bean
    @Lazy
    public IMessageSenderFactory kafkaMessageSender() {
        if (useLocalBus()) {
            return LocalBus.getInstance();
        } else {
            return new KafkaMessageProducer(bootstrapServer(), kafkaNamespacePrefix(),
                maxRequestSizeBytes, recommendedRequestSizeBytes, maxBlockMs, deliveryTimeoutMs, totalSendRetrySecs);
        }
    }
}
