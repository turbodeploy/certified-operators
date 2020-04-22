package com.vmturbo.cost.component.reserved.instance.action;

import javax.annotation.Nonnull;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.components.api.server.BaseKafkaProducerConfig;
import com.vmturbo.components.api.server.KafkaMessageProducer;
import com.vmturbo.components.common.health.KafkaProducerHealthMonitor;
import com.vmturbo.market.component.api.impl.MarketComponentNotificationReceiver;


@Configuration
@Import(BaseKafkaProducerConfig.class)
public class  ReservedInstanceActionsSenderConfig {

    @Autowired
    private BaseKafkaProducerConfig baseKafkaProducerConfig;

    @Bean
    public ReservedInstanceActionsSender actionSender() {
        return new ReservedInstanceActionsSender(baseKafkaProducerConfig.kafkaMessageSender()
                .messageSender(MarketComponentNotificationReceiver.ACTION_PLANS_TOPIC));
    }

    @Bean
    public KafkaProducerHealthMonitor kafkaHealthMonitor() {
        return new KafkaProducerHealthMonitor(baseKafkaProducerConfig.kafkaMessageSender());
    }

}
