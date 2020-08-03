package com.vmturbo.cost.component.notification;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.common.protobuf.cost.CostNotificationOuterClass.CostNotification;
import com.vmturbo.components.api.server.BaseKafkaProducerConfig;
import com.vmturbo.components.api.server.IMessageSender;
import com.vmturbo.cost.api.impl.CostComponentImpl;

/**
 * Spring configuration to import to connect to the cost component instance.
 */
@Configuration
@Import({BaseKafkaProducerConfig.class})
public class CostNotificationConfig {

    @Autowired
    private BaseKafkaProducerConfig baseKafkaProducerConfig;

    @Bean
    public CostNotificationSender costNotificationSender() {
        return new CostNotificationSender(costNotificationIMessageSender());
    }

    @Bean
    public IMessageSender<CostNotification> costNotificationIMessageSender() {
        return baseKafkaProducerConfig.kafkaMessageSender()
                .messageSender(CostComponentImpl.COST_NOTIFICATIONS);
    }

}
