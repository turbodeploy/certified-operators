package com.vmturbo.market.api;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.components.api.server.BaseKafkaProducerConfig;
import com.vmturbo.components.common.health.KafkaProducerHealthMonitor;
import com.vmturbo.market.MarketNotificationSender;

/**
 * Spring configuration to provide the {@link com.vmturbo.market.component.api.MarketComponent} integration.
 */
@Configuration
@Import(BaseKafkaProducerConfig.class)
public class MarketApiConfig {

    @Autowired
    private BaseKafkaProducerConfig baseKafkaProducerConfig;

    @Bean
    public MarketNotificationSender marketApi() {
        return MarketKafkaSender.createMarketSender(baseKafkaProducerConfig.kafkaMessageSender());
    }

    @Bean
    public KafkaProducerHealthMonitor kafkaHealthMonitor() {
        return new KafkaProducerHealthMonitor(baseKafkaProducerConfig.kafkaMessageSender());
    }

}
