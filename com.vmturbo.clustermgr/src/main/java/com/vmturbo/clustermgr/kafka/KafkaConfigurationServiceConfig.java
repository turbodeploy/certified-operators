package com.vmturbo.clustermgr.kafka;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.vmturbo.components.api.BaseKafkaConfig;

/**
 * configuration for kafka Configuration service.
 */
@Configuration
public class KafkaConfigurationServiceConfig extends BaseKafkaConfig {
    public static final int DEFAULT_CONFIG_RETRY_DELAY_MS = 30000;
    public static final int DEFAULT_CONFIG_MAX_RETRY_TIME_SECS = 300;

    // settings that control the admin client creation retry behavior
    @Value("${kafka.config.max.retry.time.secs:"+ DEFAULT_CONFIG_MAX_RETRY_TIME_SECS +"}")
    private int kafkaConfigMaxRetryTimeSecs; // max time to attempt kafka configuration

    @Value("${kafka.config.retry.delay.ms:"+ DEFAULT_CONFIG_RETRY_DELAY_MS +"}")
    private int kafkaConfigRetryDelayMs; // time between retry attempts

    @Bean
    public KafkaConfigurationService kafkaConfigurationService() {
        return new KafkaConfigurationService(bootstrapServer(), kafkaConfigMaxRetryTimeSecs,
                kafkaConfigRetryDelayMs, kafkaNamespacePrefix());
    }

}
