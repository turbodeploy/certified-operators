package com.vmturbo.action.orchestrator.api;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.action.orchestrator.api.impl.ActionOrchestratorNotificationReceiver;
import com.vmturbo.action.orchestrator.dto.ActionMessages.ActionOrchestratorNotification;
import com.vmturbo.components.api.server.BaseKafkaProducerConfig;
import com.vmturbo.components.api.server.IMessageSender;
import com.vmturbo.components.common.health.KafkaProducerHealthMonitor;

/**
 * Spring configuration to provide the {@link ActionOrchestratorNotificationSender} integration.
 */
@Configuration
@Import({BaseKafkaProducerConfig.class})
public class ActionOrchestratorApiConfig {

    @Autowired
    private BaseKafkaProducerConfig baseKafkaProducerConfig;

    @Bean
    public ActionOrchestratorNotificationSender actionOrchestratorNotificationSender() {
        return new ActionOrchestratorNotificationSender(actionOrchestratorMessageSender());
    }

    @Bean
    public IMessageSender<ActionOrchestratorNotification> actionOrchestratorMessageSender() {
        return baseKafkaProducerConfig.kafkaMessageSender()
                .messageSender(ActionOrchestratorNotificationReceiver.ACTIONS_TOPIC);
    }

    @Bean
    public KafkaProducerHealthMonitor kafkaProducerHealthMonitor() {
        return new KafkaProducerHealthMonitor(baseKafkaProducerConfig.kafkaMessageSender());
    }
}
