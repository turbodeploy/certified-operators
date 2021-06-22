package com.vmturbo.action.orchestrator.api;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.action.orchestrator.api.impl.ActionOrchestratorClientConfig;
import com.vmturbo.action.orchestrator.api.impl.ActionOrchestratorNotificationReceiver;
import com.vmturbo.action.orchestrator.dto.ActionMessages.ActionOrchestratorNotification;
import com.vmturbo.common.protobuf.action.EntitySeverityNotificationOuterClass.EntitySeverityNotification;
import com.vmturbo.components.api.server.BaseKafkaProducerConfig;
import com.vmturbo.components.api.server.IMessageSender;
import com.vmturbo.components.common.health.MessageProducerHealthMonitor;

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

    /**
     * Utility to help action orchestrator send notifications.
     *
     * @return The {@link EntitySeverityNotificationSender}.
     */
    @Bean
    public EntitySeverityNotificationSender entitySeverityNotificationSender() {
        return new EntitySeverityNotificationSender(entitySeverityNotificationMessageSender());
    }

    /**
     * Sends severity notifications over Kafka (or whatever else).
     *
     * @return The {@link IMessageSender}.
     */
    @Bean
    public IMessageSender<EntitySeverityNotification> entitySeverityNotificationMessageSender() {
        return baseKafkaProducerConfig.kafkaMessageSender()
                .messageSender(ActionOrchestratorClientConfig.SEVERITY_TOPIC);
    }

    @Bean
    public MessageProducerHealthMonitor messageProducerHealthMonitor() {
        return new MessageProducerHealthMonitor(baseKafkaProducerConfig.kafkaMessageSender());
    }
}
