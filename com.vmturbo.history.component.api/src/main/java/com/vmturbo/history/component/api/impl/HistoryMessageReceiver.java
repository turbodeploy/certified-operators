package com.vmturbo.history.component.api.impl;

import javax.annotation.Nonnull;

import com.vmturbo.components.api.client.IMessageReceiver;
import com.vmturbo.components.api.client.KafkaMessageConsumer;
import com.vmturbo.history.component.api.HistoryComponentNotifications.HistoryComponentNotification;

/**
 * Utility class to create history message receiver.
 */
public class HistoryMessageReceiver {

    /**
     * Creates message receiver for history component.
     *
     * @param kafkaMessageConsumer Kafka message consumer to get messages from
     * @return message receiver.
     */
    public static IMessageReceiver<HistoryComponentNotification> create(
            @Nonnull KafkaMessageConsumer kafkaMessageConsumer) {
        return kafkaMessageConsumer.messageReceiver(
                HistoryComponentNotificationReceiver.NOTIFICATION_TOPIC,
                HistoryComponentNotification::parseFrom);
    }
}
