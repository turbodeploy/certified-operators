package com.vmturbo.clustermgr.kafka;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

import com.vmturbo.components.api.BaseKafkaConfig;
import com.vmturbo.components.common.config.SpringConfigSource;

/**
 * configuration for kafka Configuration service.
 */
@Configuration
public class KafkaConfigurationServiceConfig extends BaseKafkaConfig {

    @Autowired
    private Environment environment;

    @Bean
    public SpringConfigSource configSource() {
        return new SpringConfigSource(environment);
    }

    @Bean
    public KafkaConfigurationService kafkaConfigurationService() {
        return new KafkaConfigurationService(configSource());
    }

}
